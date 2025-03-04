# postgresql-backup

The postgresql backup script with Python.

## Overview

This script allows you to back up your PostgreSQL databases using Python. It connects to your PostgreSQL server, performs a backup, and saves the backup file to a specified location.

## Requirements

- Python 3.x: Recommendation to run with Python Virutalenv to avoid any conflict
- PostgreSQL
- S3 bucket access right with Acces Key, IAM Role, IAM Role anywhere. Any method that allow script access s3 via boto3

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/postgresql-backup.git
    ```
2. Navigate to the project directory:
    ```sh
    cd postgresql-backup
    ```
3. Install the required dependencies:
    ```sh
    pip3 install -r requirements.txt
    ```

## Usage

1. Run the backup script with the required arguments:
    ```sh
    python3 x-backup.py --bucket your_bucket --password your_gpg_password [OPTIONS]
    ```

    Example:
    ```sh
    python3 x-backup.py --bucket my-backup-bucket --password myGPGpassword --host localhost --dbname mydatabase
    ```

    Required arguments:
    - `--bucket`: The S3 bucket name where backups will be uploaded
    - `--password`: The GPG password for encryption

    Optional arguments (run `python3 x-backup.py --help` for complete list):
    - `--host`: PostgreSQL server hostname
    - `--dbname`: Database name to backup
    - `--user`: PostgreSQL user
    - `--backup-dir`: Local directory for temporary backup files
    - `--region`: AWS region for S3 bucket
    - `--profile`: AWS profile name

## Testing

Tested on

OS:
- Ubuntu 20.04
- Ubuntu 22.04
- Ubuntu 24.04

Postgres version: 13,14,15

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.
