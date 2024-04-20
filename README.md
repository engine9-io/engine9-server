# Engine9 CRM Server

Contains:
    data-server: Web server to deliver configurations and data to browsers


## Installation:

Git clone engine9-server to a directory

Make an engine9 accounts directory (e.g. /etc/engine9/accounts).  This directory holds long-term files

Make a /var/log/engine9 directory for log files

Copy .env.template to .env
Update .env settings

### Packet server -- CloudZip
Sadly, most of the Node.js libraries for zip don't support some high performance features, such as extracting partial data from remote zip files.  To allow for this major feature, Engine9 servers need to have CloudZip (https://github.com/ozkatz/cloudzip) installed.  This requires Go, and some compilation efforts, but is necessary until better Node.js unzip libraries come along.  See the packet-server directory for more information.


