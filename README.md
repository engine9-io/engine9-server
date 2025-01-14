# Engine9 CRM Server

Contains:
    data-server: Web server to deliver configurations and data to browsers

## Requirements
Engine9 is developed and deployed using Node.js/NPM.  Node.js versions > 20.0 are required.

Engine9 also requires a SQL database.  Currently modern (Post-2022) MySQL and variants (e.g. MariaDB) are supported. 

There are also number of optional components, such as cloud based file stores, etc, that are covered elsewhere.

## Installation:

Create a local directory

`mkdir $HOME/engine9`

`cd $HOME/engine9`

Clone the server code:

`git clone https://github.com/engine9-io/engine9-server.git`

The resulting directory will be known as ENGINE9_SERVER_DIR, e.g.

You can set this as an environment variable for use with the command line tools, for example adding the following to your .zprofile (in recent Mac OS versions)

`export ENGINE9_SERVER_DIR=/home/engine9-user/engine9/engine9-server
PATH=$ENGINE9_SERVER_DIR/bin:$PATH`

and restarting your terminal.  This will also add the /bin directory for the engine9 command line tools.


### NPM install
Change to the $ENGINE9_SERVER_DIR, and run

`npm install`

to install all supporting libraries.

### Configuration Files
#### .env
The .env file contains global system settings for all accounts. To deploy, copy `$ENGINE9_SERVER_DIR/.env.template` to `$ENGINE9_SERVER_DIR/.env`

#### account-config.json
The account configuration contains per-account settings, allowing for different credentials, databases, etc, per account.

Copy `$ENGINE9_SERVER_DIR/account-config.template.json` to `$ENGINE9_SERVER_DIR/account-config.json`.

### Required Directories
Make an engine9 data directory (e.g. /etc/engine9).  This directory holds a variety of different Engine9 files intended for long term storage. 

#### Accounts
Long term and per-account configuration files

`sudo mkdir -p /etc/engine9/accounts`

#### Stored Inputs
`Stored Inputs` are a partially shared location where input data can be dropped by outside entities, and read and processed by Engine9.

`sudo mkdir -p /etc/engine9/stored_inputs`

#### Logs
`sudo mkdir -p /var/log/engine9`

#### Directory Permissions
Ensure the current user can access the above directories

`sudo chown -R $USER /etc/engine9 /var/log/engine9`


### Command line interface -- aka CLI
Much of the deployment and testing for Engine9 can be accomplished with the `e9` command line tool.

To use it, make sure the path:

`$ENGINE9_SERVER_DIR/bin`

is in your path. See above for an example.

The syntax for e9 is :

`e9 <worker-match> <method-match> <--options>`

where `worker-match` is a string matcher for the name of the worker, and `method-match` is the method name.

To test that it works, try:

`e9 echo echo --foo=bar`

which should return some logging, then:

`{ foo: 'bar', last_run: 2024-12-29T16:20:22.760Z }`

The EchoWorker is a testing worker that merely returns what you pass it, and is useful to validate your environment is working as expected.


### Database
Engine9 currently supports modern MySQL variants, such as MariaDB.
Create a database, username, and secure password for the `engine9` account using a secure password generator.
Optionally create a sibling database for the `test` account if you wish to run engine9 development tests.

Update the account-config.json file with the SQL credentials for each account.

You can test your sql connection with 

`e9 sql ok`

which should return

`{ ok: 1 }`


## Plugins
Engine9 is driven by plugins.  Plugins contain database schemas, transforms, UI configuration, and a variety of other things.

There are four main types of plugins you'll encounter in Engine9.  The type typically represents the location and installation process of a plugin.

- **local** - Runs only for a given account, configuration happens exclusively in the database.  Custom fields for people or transactions are a good example of a local plugin.
- **remote** - Configurations are hosted elsewhere, typically on a platform such as GitHub, and are shared with versions across many accounts.  There is a canonical 'path', often a URL, that uniquely identifies a plugin.  Remote plugins can be shared on the Engine9 Marketplace, or be private amongst a group of accounts.
- **interface** - A subset of remote plugins that are available to all Engine9 accounts, and are used to ensure common standardized fields.  The path starts with `@engine9-interfaces/...`, and are hosted 
- **file** - Only used when developing on a single machine, and not suitable for production.

     


### Initial Plugins - Standard Interfaces
  `Interfaces` are a standardized subset of plugins that are intended to be used as a standard across plugins, and allow plugins to leverage common concepts in Engine9, like `person` and `message`.  Every interface is a plugin, but not every plugin is an interface.  You can use these standardized plugins as part of the initial deployment of Engine9, to produce the database tables you'll need to get started.

https://github.com/engine9-io/engine9-interfaces

You can use the cli command:

`e9 schema deploy --schema=<schema path>`

where the schema path can be a local file, or a path that looks like `@engine9-interfaces/<name>`, which will retrieve the most recent version of an interface from the GitHub repository.

To get started, deploy a couple common plugins, such as


#### Plugin metadata and identifiers
`e9 schema deploy --schema=@engine9-interfaces/plugin`

#### Core person and identity tables

`e9 schema deploy --schema=@engine9-interfaces/person`

#### Core person email addresses

`e9 schema deploy --schema=@engine9-interfaces/person_email`

#### Core message tables
Framework for outbound communications like email, online ads, and SMS

`e9 schema deploy --schema=@engine9-interfaces/message`





