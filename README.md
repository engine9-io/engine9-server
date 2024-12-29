# Engine9 CRM Server

Contains:
    data-server: Web server to deliver configurations and data to browsers

## Installation:
Git clone engine9-server to a local directory

### Configuration Files
#### .env
The .env file contains global system settings for all accounts. To deploy:
copy .env.template to .env

#### account-config.json
The account configuration contains per-account settings, allowing for different credentials, databases, etc, per account.

Copy account-config.template.json to account-config.json

### Required Directories
Make an engine9 data directory (e.g. /etc/engine9).  This directory holds a variety of different Engine9 files intended for long term storage. 

#### Accounts
Long term and per-account configuration files
sudo mkdir -p /etc/engine9/accounts

#### Stored Inputs
`Stored Inputs` are a partially shared location where input data can be dropped by outside entities, and read and processed by Engine9.  This can also be an S3 location.

sudo mkdir -p /etc/engine9/stored_inputs 

#### Logs
sudo mkdir -p /var/log/engine9

#### Directory Permissions
Ensure the current user can access
sudo chown -R $USER /etc/engine9 /var/log/engine9


### Database
Engine9 currently supports modern MySQL variants, such as MariaDB.
Create a database, username, and secure password for the `engine9` account using a secure password generator.
Optionally create a sibling database for the `test` account if you wish to run engine9 development tests.

Update the account-config.json file with the credentials for each account.


