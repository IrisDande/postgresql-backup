#!/usr/bin/env python3

import os
import sys
import subprocess
from datetime import datetime
import boto3
import argparse
import time
from botocore.exceptions import ClientError
import requests
import logging
import getpass
import atexit
import fcntl
import errno
import socket
import glob
import io
import threading
import queue

class PostgreSQLBackup:
    def __init__(self, args=None):
        # Configuration from environment variables with defaults
        self.db_user = os.getenv('PGUSER', 'postgres')
        self.s3_bucket = os.getenv('S3_BUCKET', 'default-backup-bucket')
        self.hostname = socket.getfqdn()
        self.s3_prefix = os.getenv('S3_PREFIX', f'postgresql-backups/{self.hostname}/')
        if not self.s3_prefix.endswith('/'):
            self.s3_prefix += '/'
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.initial_backoff = int(os.getenv('INITIAL_BACKOFF', '1'))
        self.backup_password = os.getenv('BACKUP_PASSWORD', '')
        self.lock_file = os.getenv('LOCK_FILE', '/tmp/postgresql_backup.lock')
        self.backup_dir = os.getenv('BACKUP_DIR', '/backup')
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
        self.keep_local = int(os.getenv('KEEP_LOCAL_BACKUPS', '7'))
        self.aws_region = os.getenv('AWS_REGION', 'ap-southeast-1')
        self.required_user = os.getenv('REQUIRED_USER', 'root')
        self.required_packages = ['gpg', 'zstd']  # Changed from xz to zstd
        self.zstd_threads = int(os.getenv('ZSTD_THREADS', '4'))
        self.zstd_level = 19  # Default compression level
        
        # Setup logging
        self.setup_logging()
        
        # Override with command line arguments if provided
        self.apply_arguments(args)
            
        # Initialize lock file descriptor
        self.lock_fd = None

    def setup_logging(self):
        """Configure root logger for all modules"""
        log_dir = os.getenv('LOG_DIR', '/var/log/pgdump')
        os.makedirs(log_dir, exist_ok=True)

        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"{log_dir}/postgresql_backup.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def apply_arguments(self, args):
        if args.user:
            self.db_user = args.user
        if args.bucket:
            self.s3_bucket = args.bucket
        if args.prefix:
            self.s3_prefix = args.prefix
        if args.verbose:
            # Enable debug logging for all modules
            logging.getLogger().setLevel(logging.DEBUG)
        if args.password:
            self.backup_password = args.password
        if args.backup_dir:
            self.backup_dir = args.backup_dir
        if args.keep_local:
            self.keep_local = args.keep_local
        if args.region:
            self.aws_region = args.region
        if args.threads:
            self.zstd_threads = args.threads
        if args.compression_level is not None:
            self.zstd_level = args.compression_level
        if args.slack_webhook:
            self.slack_webhook_url = args.slack_webhook

    def run_subprocess_command(self, command, error_msg="Command failed"):
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logging.error(f"{error_msg}: {str(e)}")
            logging.error(f"Command output: {e.stderr}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error running command: {str(e)}")
            raise

    def send_slack_message(self, subject, message):
        if not self.slack_webhook_url:
            logging.warning("SLACK_WEBHOOK_URL not set, skipping notification")
            return
        try:
            payload = {
                "text": f"*{subject}*\n```{message}```"
            }
            response = requests.post(self.slack_webhook_url, json=payload)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Failed to send Slack message: {str(e)}")

    def upload_with_retry(self, s3_client, file_path, bucket, key):
        backoff = self.initial_backoff
        for attempt in range(self.max_retries):
            try:
                s3_client.upload_file(
                    file_path,
                    bucket,
                    key,
                    ExtraArgs={"StorageClass": "STANDARD_IA"}
                )
                return True
            except ClientError as e:
                if attempt == self.max_retries - 1:
                    raise e
                logging.warning(f"Upload attempt {attempt + 1} failed, retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2
        return False

    def get_all_databases(self):
        cmd = f"sudo -u postgres psql -l -t | cut -d'|' -f1 | grep -v template0 | grep -v template1 | grep -v postgres"
        result = self.run_subprocess_command(cmd)
        return [db.strip() for db in result.split('\n') if db.strip()]

    def encrypt_file(self, input_file, output_file, password):
        """Encrypt file using GPG symmetric encryption"""
        try:
            cmd = f"gpg --batch --yes --passphrase {password} -c --output {output_file} {input_file}"
            self.run_subprocess_command(cmd, "Encryption failed")
            return True
        except Exception as e:
            logging.error(f"Encryption failed: {str(e)}")
            return False

    def check_package_installed(self, package):
        """Check if a package is installed"""
        try:
            self.run_subprocess_command(f"which {package}", f"{package} not found")
            return True
        except:
            return False

    def install_packages(self, packages):
        """Install packages using available package manager"""
        try:
            if self.run_subprocess_command("which apt-get", "apt-get not found"):
                logging.info(f"Installing packages using apt: {packages}")
                packages_str = ' '.join(packages)
                self.run_subprocess_command(f"sudo apt-get update && sudo apt-get install -y {packages_str}")
            else:
                raise Exception("No supported package manager found (apt/yum)")
            return True
        except Exception as e:
            logging.error(f"Failed to install packages: {str(e)}")
            return False

    def ensure_packages_installed(self):
        """Ensure required packages are installed"""
        missing_packages = [pkg for pkg in self.required_packages if not self.check_package_installed(pkg)]
        
        if missing_packages:
            logging.info(f"Missing packages found: {missing_packages}")
            if not self.install_packages(missing_packages):
                raise Exception(f"Failed to install required packages: {missing_packages}")
            
            # Verify installation
            still_missing = [pkg for pkg in missing_packages if not self.check_package_installed(pkg)]
            if still_missing:
                raise Exception(f"Package installation verification failed for: {still_missing}")
            
            logging.info("All required packages installed successfully")

    def cleanup_old_backups(self, db_name):
        """Clean up old backups keeping only the specified number of recent ones"""
        # Validate keep_local value
        if self.keep_local < 1:
            logging.warning("Invalid keep_local value, setting to default (7)")
            self.keep_local = 7

        pattern = os.path.join(self.backup_dir, f"backup_{db_name}_*.sql.zst*")  # Changed from .xz to .zst
        backups = sorted(glob.glob(pattern), key=os.path.getctime, reverse=True)
        
        if len(backups) > self.keep_local:
            logging.info(f"Found {len(backups)} backups, keeping {self.keep_local} most recent")
            for old_backup in backups[self.keep_local:]:
                try:
                    os.remove(old_backup)
                    logging.info(f"Removed old backup: {old_backup}")
                except Exception as e:
                    logging.warning(f"Failed to remove old backup {old_backup}: {str(e)}")
        else:
            logging.info(f"Found {len(backups)} backups, no cleanup needed (keep_local={self.keep_local})")

    def verify_aws_access(self):
        """Verify AWS credentials and bucket access"""
        try:
            logging.info("Verifying AWS credentials and bucket access...")
            session = boto3.Session(profile_name='default')
            s3_client = session.client('s3', region_name=self.aws_region)
            # Test bucket access by listing objects

            s3_client.head_bucket(Bucket=self.s3_bucket)
            
            # Try to list objects with prefix to verify permissions
            s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=self.s3_prefix,
                MaxKeys=1
            )
            logging.info("AWS credentials and bucket access verified")
            return s3_client
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '403':
                logging.error(f"AWS credentials are invalid or lack required permissions to access bucket: {self.s3_bucket}")
            elif error_code == '404':
                logging.error(f"S3 bucket '{self.s3_bucket}' does not exist")
            else:
                logging.error(f"AWS access error: {str(e)} to bucket: {self.s3_bucket}")
            raise

    def upload_part_with_retry(self, s3_client, bucket, key, part_number, upload_id, data):
        """Upload a single part with retry logic"""
        backoff = self.initial_backoff
        for attempt in range(self.max_retries):
            try:
                part = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                return part['ETag']
            except ClientError as e:
                if attempt == self.max_retries - 1:
                    raise
                logging.warning(f"Part {part_number} upload failed, attempt {attempt + 1}/{self.max_retries}, retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2

    def stream_to_s3(self, process, s3_client, bucket, key):
        """Stream data directly to S3 using multipart upload with retries"""
        mpu = None
        try:
            # Initialize multipart upload with retry
            backoff = self.initial_backoff
            for attempt in range(self.max_retries):
                try:
                    mpu = s3_client.create_multipart_upload(
                        Bucket=bucket,
                        Key=key,
                        StorageClass='STANDARD_IA'
                    )
                    break
                except ClientError as e:
                    if attempt == self.max_retries - 1:
                        raise
                    logging.warning(f"Create multipart upload failed, attempt {attempt + 1}/{self.max_retries}, retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2

            parts = []
            part_number = 1
            
            while True:
                data = process.stdout.read(8 * 1024 * 1024)
                if not data:
                    break
                
                # Upload part with retry
                etag = self.upload_part_with_retry(
                    s3_client, bucket, key,
                    part_number, mpu['UploadId'],
                    data
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': etag
                })
                part_number += 1
            
            # Complete multipart upload with retry
            backoff = self.initial_backoff
            for attempt in range(self.max_retries):
                try:
                    s3_client.complete_multipart_upload(
                        Bucket=bucket,
                        Key=key,
                        UploadId=mpu['UploadId'],
                        MultipartUpload={'Parts': parts}
                    )
                    break
                except ClientError as e:
                    if attempt == self.max_retries - 1:
                        raise
                    logging.warning(f"Complete multipart upload failed, attempt {attempt + 1}/{self.max_retries}, retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
            
        except Exception as e:
            if mpu:
                # Abort multipart upload with retry
                backoff = self.initial_backoff
                for attempt in range(self.max_retries):
                    try:
                        s3_client.abort_multipart_upload(
                            Bucket=bucket,
                            Key=key,
                            UploadId=mpu['UploadId']
                        )
                        break
                    except ClientError as abort_e:
                        if attempt == self.max_retries - 1:
                            logging.error(f"Failed to abort multipart upload: {str(abort_e)}")
                        logging.warning(f"Abort multipart upload failed, attempt {attempt + 1}/{self.max_retries}, retrying in {backoff}s...")
                        time.sleep(backoff)
                        backoff *= 2
            raise e

    def create_backup(self, db_name=None):
        """Ensure required packages and AWS access before starting backup"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.ensure_packages_installed()
        
        # Verify AWS access first
        try:
            s3_client = self.verify_aws_access()
        except Exception as e:
            logging.error(f"Failed to verify AWS access: {str(e)}")
            subject = f"{self.hostname}: PostgreSQL Backup Errors - {timestamp}"
            self.send_slack_message(subject, str(e))
            sys.exit(1)
        
        databases = [db_name] if db_name else self.get_all_databases()
        errors = []
        
        # Ensure backup directory exists
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Get password if not set in environment
        password = self.backup_password
        if not password:
            password = getpass.getpass('Enter backup encryption password: ')
        logging.info(f"The databases to backup are: {databases}")
        for db in databases:
            s3_key = f"{self.s3_prefix}backup_{db}_{timestamp}.sql.zst.gpg"
            
            try:
                # Create backup pipeline: pg_dump | zstd | gpg
                logging.info(f"Creating and streaming backup of {db}")
                cmd = f"sudo -u postgres pg_dump {db} | zstd -T{self.zstd_threads} -{self.zstd_level} | gpg --batch --yes --passphrase {password} -c"
                
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                # Stream to S3
                logging.info(f"Streaming backup to s3://{self.s3_bucket}/{s3_key}")
                self.stream_to_s3(process, s3_client, self.s3_bucket, s3_key)
                
                # Check process exit status
                _, stderr = process.communicate()
                if process.returncode != 0:
                    raise Exception(f"Backup pipeline failed: {stderr.decode()}")
                
                logging.info(f"Backup of {db} completed and uploaded to s3://{self.s3_bucket}/{s3_key}")
                
            except Exception as e:
                error_msg = f"Backup/upload failed for {db}: {str(e)}"
                errors.append(error_msg)
                logging.error(error_msg)
                continue
        
        if errors:
            subject = f"{self.hostname}: PostgreSQL Backup Errors - {timestamp}"
            message = "\n".join(errors)
            self.send_slack_message(subject, message)

    def acquire_lock(self):
        """Try to acquire a lock file for the backup process"""
        try:
            lock_fd = open(self.lock_file, 'w')
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write PID to lock file
            lock_fd.write(str(os.getpid()))
            lock_fd.flush()
            self.lock_fd = lock_fd
            return lock_fd
        except IOError as e:
            if e.errno == errno.EAGAIN:
                # Another instance is running
                logging.error("Another backup process is already running")
                sys.exit(1)
            raise

    def release_lock(self):
        """Release the lock file"""
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
                os.unlink(self.lock_file)
            except Exception as e:
                logging.warning(f"Error releasing lock: {str(e)}")

    @staticmethod
    def parse_arguments():
        hostname = socket.getfqdn()
        parser = argparse.ArgumentParser(description='PostgreSQL backup script with S3 upload')
        parser.add_argument('--database', '-d', 
                            help='Specific database to backup. If not provided, all databases will be backed up')
        parser.add_argument('--user', '-u',
                            help='Database user (overrides default)')
        parser.add_argument('--bucket', '-b',
                            help='S3 bucket name (overrides default: default-backup-bucket)')
        parser.add_argument('--prefix', '-p',
                            help=f'S3 prefix/folder (overrides default: postgresql-backups/{hostname}/)')
        parser.add_argument('--verbose', '-v', action='store_true',
                            help='Enable verbose logging')
        parser.add_argument('--password', 
                           help='Encryption password (if not provided, will prompt or use BACKUP_PASSWORD env var)')
        parser.add_argument('--backup-dir', '-bd',
                           help='Directory to store backup files (overrides default: /backup)')
        parser.add_argument('--keep-local', '-k', type=int,
                          help='Number of local backups to keep (default: 7)')
        parser.add_argument('--region', '-r',
                          help='AWS region (default: ap-southeast-1)')
        parser.add_argument('--threads', '-t', type=int,
                          help='Number of threads for zstd compression (default: 4)')
        parser.add_argument('--compression-level', '-c', type=int,
                          help='ZSTD compression level (1-22, default: 22)',
                          choices=range(1, 22))
        parser.add_argument('--slack-webhook', '-s',
                          help='Slack webhook URL for notifications (overrides SLACK_WEBHOOK_URL env var)')
        return parser.parse_args()

def main():
    args = PostgreSQLBackup.parse_arguments()
    
    backup = PostgreSQLBackup(args)
    backup.acquire_lock()
    atexit.register(backup.release_lock)
    
    # Ensure backup directory exists before changing to it
    try:
        os.makedirs(backup.backup_dir, exist_ok=True)
        os.chdir(backup.backup_dir)
    except OSError as e:
        logging.error(f"Failed to create or access backup directory {backup.backup_dir}: {str(e)}")
        sys.exit(1)
        
    backup.create_backup(args.database)

if __name__ == "__main__":
    main()