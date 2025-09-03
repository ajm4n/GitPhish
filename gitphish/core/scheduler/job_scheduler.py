"""
Job Scheduler for GitPhish - Handles scheduled campaign execution.
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from enum import Enum
import schedule
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobType(Enum):
    SMS_CAMPAIGN = "sms_campaign"
    AZURE_CAMPAIGN = "azure_campaign"

class JobScheduler:
    """Handles scheduling and execution of GitPhish campaigns."""
    
    def __init__(self, db_path: str = "data/gitphish.db"):
        self.db_path = db_path
        self.running = False
        self.scheduler_thread = None
        self.executor = ThreadPoolExecutor(max_workers=3)  # Allow up to 3 concurrent jobs to prevent resource exhaustion
        self.running_jobs = {}  # Track running job futures
        self._init_database()
        
    def _init_database(self):
        """Initialize the jobs database table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS scheduled_jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_type TEXT NOT NULL,
                        job_name TEXT NOT NULL,
                        scheduled_time TEXT NOT NULL,
                        job_data TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        executed_at TEXT,
                        error_message TEXT,
                        campaign_id TEXT,
                        result_data TEXT
                    )
                """)
                conn.commit()
                logger.info("Scheduled jobs database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize jobs database: {e}")
    
    def schedule_job(self, job_type: JobType, job_name: str, scheduled_time: datetime, job_data: Dict[str, Any]) -> str:
        """Schedule a new job for execution."""
        try:
            job_id = f"{job_type.value}_{int(time.time())}"
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO scheduled_jobs 
                    (job_type, job_name, scheduled_time, job_data, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    job_type.value,
                    job_name,
                    scheduled_time.isoformat(),
                    json.dumps(job_data),
                    JobStatus.PENDING.value
                ))
                conn.commit()
                
            logger.info(f"Scheduled job {job_id} for {scheduled_time}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to schedule job: {e}")
            raise
    
    def get_scheduled_jobs(self, status: Optional[JobStatus] = None) -> List[Dict[str, Any]]:
        """Get list of scheduled jobs, optionally filtered by status."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                if status:
                    cursor = conn.execute("""
                        SELECT * FROM scheduled_jobs 
                        WHERE status = ? 
                        ORDER BY scheduled_time ASC
                    """, (status.value,))
                else:
                    cursor = conn.execute("""
                        SELECT * FROM scheduled_jobs 
                        ORDER BY scheduled_time ASC
                    """)
                
                jobs = []
                for row in cursor.fetchall():
                    job = dict(row)
                    job['job_data'] = json.loads(job['job_data'])
                    if job['result_data']:
                        job['result_data'] = json.loads(job['result_data'])
                    jobs.append(job)
                
                return jobs
                
        except Exception as e:
            logger.error(f"Failed to get scheduled jobs: {e}")
            return []
    
    def cancel_job(self, job_id: int) -> bool:
        """Cancel a scheduled job."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    UPDATE scheduled_jobs 
                    SET status = ? 
                    WHERE id = ? AND status = ?
                """, (JobStatus.CANCELLED.value, job_id, JobStatus.PENDING.value))
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"Cancelled job {job_id}")
                    return True
                else:
                    logger.warning(f"Job {job_id} not found or not cancellable")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False
    
    def _execute_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a scheduled job."""
        job_id = job['id']
        job_type = JobType(job['job_type'])
        job_data = job['job_data']
        
        logger.info(f"Executing job {job_id}: {job['job_name']}")
        
        # Update status to running
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE scheduled_jobs 
                SET status = ?, executed_at = ? 
                WHERE id = ?
            """, (JobStatus.RUNNING.value, datetime.now().isoformat(), job_id))
            conn.commit()
        
        try:
            # Build command based on job type
            if job_type == JobType.SMS_CAMPAIGN:
                cmd_args = self._build_sms_command(job_data)
            elif job_type == JobType.AZURE_CAMPAIGN:
                cmd_args = self._build_azure_command(job_data)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
            
            # Set up environment
            env = os.environ.copy()
            if job_data.get('provider') == 'aws':
                if job_data.get('awsAccessKeyId'):
                    env['AWS_ACCESS_KEY_ID'] = job_data['awsAccessKeyId']
                if job_data.get('awsSecretAccessKey'):
                    env['AWS_SECRET_ACCESS_KEY'] = job_data['awsSecretAccessKey']
                if job_data.get('awsSessionToken'):
                    env['AWS_SESSION_TOKEN'] = job_data['awsSessionToken']
                if job_data.get('awsRegion'):
                    env['AWS_DEFAULT_REGION'] = job_data['awsRegion']
            
            # Execute the command
            process = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                env=env,
                timeout=3600  # 1 hour timeout
            )
            
            # Extract real campaign ID from GitPhish output
            campaign_id = self._extract_campaign_id_from_output(process.stdout, job_type.value, job_id)
            
            result_data = {
                'campaign_id': campaign_id,
                'return_code': process.returncode,
                'stdout': process.stdout,
                'stderr': process.stderr,
                'command': ' '.join(cmd_args)
            }
            
            # Update job status
            if process.returncode == 0:
                status = JobStatus.COMPLETED
                error_message = None
                logger.info(f"Job {job_id} completed successfully")
            else:
                status = JobStatus.FAILED
                error_message = f"Command failed with return code {process.returncode}: {process.stderr}"
                logger.error(f"Job {job_id} failed: {error_message}")
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE scheduled_jobs 
                    SET status = ?, error_message = ?, campaign_id = ?, result_data = ?
                    WHERE id = ?
                """, (
                    status.value,
                    error_message,
                    campaign_id,
                    json.dumps(result_data),
                    job_id
                ))
                conn.commit()
            
            return result_data
            
        except Exception as e:
            error_message = f"Job execution failed: {str(e)}"
            logger.error(f"Job {job_id} failed: {error_message}")
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE scheduled_jobs 
                    SET status = ?, error_message = ? 
                    WHERE id = ?
                """, (JobStatus.FAILED.value, error_message, job_id))
                conn.commit()
            
            return {'error': error_message}
    
    def _build_sms_command(self, job_data: Dict[str, Any]) -> List[str]:
        """Build SMS campaign command."""
        provider = job_data['provider']
        platform = job_data.get('platform', 'github')
        
        cmd = ['python', '-m', 'gitphish', 'sms']
        
        if provider == 'twilio' and platform == 'github':
            cmd.append('twilio-github')
        elif provider == 'aws' and platform == 'github':
            cmd.append('aws-github')
        else:
            raise ValueError(f"Unsupported SMS campaign: {provider}-{platform}")
        
        # Add required arguments
        cmd.extend(['-e', job_data['targetEmail']])
        cmd.extend(['-p', job_data['targetPhone']])
        
        if provider == 'twilio':
            cmd.extend(['--sid', job_data['twilioSid']])
            cmd.extend(['--token', job_data['twilioToken']])
            cmd.extend(['--from-phone', job_data['fromPhone']])
        elif provider == 'aws':
            cmd.extend(['--region', job_data.get('awsRegion', 'us-east-2')])
        
        if job_data.get('scope'):
            cmd.extend(['--scope', job_data['scope']])
        if job_data.get('messageTemplate'):
            cmd.extend(['--message', job_data['messageTemplate']])
        if job_data.get('debug'):
            cmd.append('--debug')
        
        return cmd
    
    def _build_azure_command(self, job_data: Dict[str, Any]) -> List[str]:
        """Build Azure campaign command."""
        provider = job_data['provider']
        platform = job_data.get('platform', 'azure')
        
        cmd = ['python', '-m', 'gitphish', 'azure']
        
        if provider == 'twilio' and platform == 'azure':
            cmd.append('twilio-azure')
        elif provider == 'aws' and platform == 'azure':
            cmd.append('aws-azure')
        else:
            raise ValueError(f"Unsupported Azure campaign: {provider}-{platform}")
        
        # Add required arguments
        cmd.extend(['-e', job_data['targetEmail']])
        cmd.extend(['-p', job_data['targetPhone']])
        
        if provider == 'twilio':
            cmd.extend(['--sid', job_data['twilioSid']])
            cmd.extend(['--token', job_data['twilioToken']])
            cmd.extend(['--from-phone', job_data['fromPhone']])
        elif provider == 'aws':
            cmd.extend(['--region', job_data.get('awsRegion', 'us-east-2')])
        
        if job_data.get('scope'):
            cmd.extend(['--scope', job_data['scope']])
        if job_data.get('messageTemplate'):
            cmd.extend(['--message', job_data['messageTemplate']])
        if job_data.get('debug'):
            cmd.append('--debug')
        
        return cmd
    
    def _scheduler_loop(self):
        """Main scheduler loop that runs in background thread."""
        logger.info("Scheduler loop started")
        
        while self.running:
            try:
                # Get pending jobs that are due to run
                from datetime import timezone
                now = datetime.now(timezone.utc).replace(tzinfo=None)  # Get UTC time as naive
                pending_jobs = self.get_scheduled_jobs(JobStatus.PENDING)
                
                # Clean up completed jobs
                completed_jobs = []
                for job_id, future in self.running_jobs.items():
                    if future.done():
                        completed_jobs.append(job_id)
                        try:
                            result = future.result()  # Get result or exception
                        except Exception as e:
                            logger.error(f"Job {job_id} completed with error: {e}")
                
                # Remove completed jobs from tracking
                for job_id in completed_jobs:
                    del self.running_jobs[job_id]
                
                for job in pending_jobs:
                    scheduled_time = datetime.fromisoformat(job['scheduled_time'])
                    # Ensure both datetimes are naive (no timezone info) for comparison
                    if scheduled_time.tzinfo is not None:
                        scheduled_time = scheduled_time.replace(tzinfo=None)
                    
                    # Check if job is due to run and not already running
                    job_id = job['id']
                    if scheduled_time <= now and job_id not in self.running_jobs:
                        logger.info(f"Executing scheduled job {job_id}: {job['job_name']} in background thread")
                        try:
                            # Submit job to thread pool for parallel execution
                            future = self.executor.submit(self._execute_job, job)
                            self.running_jobs[job_id] = future
                            logger.info(f"Job {job_id} submitted to thread pool. Currently running: {len(self.running_jobs)} jobs")
                        except Exception as e:
                            logger.error(f"Failed to submit job {job_id} to thread pool: {e}")
                
                # Sleep for 30 seconds before checking again
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                time.sleep(60)  # Wait longer on error
    
    def start(self):
        """Start the job scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        logger.info("Job scheduler started")
    
    def stop(self):
        """Stop the job scheduler."""
        if not self.running:
            logger.warning("Scheduler is not running")
            return
        
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        # Shutdown thread pool executor
        self.executor.shutdown(wait=False)
        logger.info("Job scheduler stopped")
    
    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM scheduled_jobs WHERE id = ?
                """, (job_id,))
                
                row = cursor.fetchone()
                if row:
                    job = dict(row)
                    job['job_data'] = json.loads(job['job_data'])
                    if job['result_data']:
                        job['result_data'] = json.loads(job['result_data'])
                    return job
                return None
                
        except Exception as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            return None
    
    def _extract_campaign_id_from_output(self, stdout: str, job_type: str, job_id: int) -> str:
        """Extract actual campaign ID from GitPhish command output."""
        try:
            # Try to find campaign ID patterns in the output
            import re
            
            # Look for campaign ID patterns in different formats
            patterns = [
                r'Campaign ID[:\s]*([a-zA-Z0-9_-]+)',
                r'Campaign[:\s]*([a-zA-Z0-9_-]+)',
                r'Started campaign[:\s]*([a-zA-Z0-9_-]+)',
                r'ID[:\s]*([a-zA-Z0-9_-]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, stdout, re.IGNORECASE)
                if match:
                    campaign_id = match.group(1).strip()
                    logger.info(f"Extracted campaign ID from output: {campaign_id}")
                    return campaign_id
            
            # If no campaign ID found in output, generate a fallback ID
            fallback_id = f"{job_type}_{job_id}_{int(time.time())}"
            logger.warning(f"No campaign ID found in output, using fallback: {fallback_id}")
            return fallback_id
            
        except Exception as e:
            # If anything goes wrong, use fallback ID
            fallback_id = f"{job_type}_{job_id}_{int(time.time())}"
            logger.error(f"Error extracting campaign ID: {e}, using fallback: {fallback_id}")
            return fallback_id