Configuration
=============

[Main]
The `tmp` option in [Main] is the tmp directory you would like to use to
store temorary backup files. Usually /tmp is fine for this.

[Backup]
All configuration in the [Backup] section are required except `inc_path`.
The MySQL user specified in the needs SELECT and LOCK TABLE permissions
on the database you are going to dump.

[Logging]
In [Logging] `log_path` is the path and file name to the log file that will
capture all the information, warning and error output as the backups run.
The file will need to be writable by the user running the application.

[Encryption]
If `enabled` in [Encryption] is set to 'true' the backups will be encrypted
using a pgp or gnupg public key you specify in `key_name`. If you are running
the backup application as root, you will need to create a public key with your
normal user and import the key into roots key ring.

[Fetch]
In the [Fetch] section required a remove ssh target string in `connection_string`.
This will be the user and destination of the remote server...

    connection_string = user@example.com
    port = 22

You are also required to specify the `remote_full_path` and `remote_inc_path` and 
then a `local_save_path`. This will scp backups files over from the remote machine
and create a single .sql file that can be used to restore your database. The file
will be stored in your `local_save_path` directory.

Turning on Binary Logging
-------------------------

In order for this application to work you need to turn on bin logging.

    sudo vi /etc/mysql/my.cnf

inside `my.cnf` you will see...

    # log_bin = /var/log/mysql/mysql-bin.log

Uncommenting that line will enable loggin for you. You then need to restart your
MySQL server...

    # on Debian/Ubuntu...
    sudo /etc/init.d/mysql restart

Useage
======

WARNING: DO NOT run multiple instances of this application against the same database server!
Binary logs are made by tracking everything MySQL does. Running more than one instance of this
application will cause confusion in bin-log sequences and leave you with corrupt database
backups.

Full backup
-----------

A full dump of the MySQL database can be run by issueing the following
command. This is required for incremental backups to run.

    $ pmb.py backup --full
    $ pmb.py backup -f

You can also specify a database to back up using --database= or --all-databases

    $ pmb.py backup --full --all-databases
    $ pmb.py backup --full --database=my_database

Incremental backup
------------------

Incremental backups will check the date and look for a full backup
that was run for the day. If no full backup was run, it will throw
and exception and exit with a fatal error.

    $ pmb.py backup --incremental
    $ pmb.py backup -i

As with a full backup, you can specify the database you would like
to backup using --database= or --all-databases

    $ pmb.py backup --incremental --all-databases
    $ pmb.py backup --incremental --database=my_database

Restore database from backup set
--------------------------------

    $ pmb.py restore --date=YYYYMMDD --time=HHMM

You can restore a specific database by using --database=.

    $ pmb.py restore --database=my_database --date=YYYYMMDD --time=HHMM

Fetching a backup from a remote server
--------------------------------------

    $ pmb.py fetch --date=YYYMMDD --time=HHMM

Running under cron
------------------

The best way to run under cron is to run a full backup at midnight (00:00)
and to run incrementals every 15 minutes to capture 4 backups an hour...

Open /etc/crontab as root or using sudo...

    0 0 * * * root /path/to/pmb.py backup --full --quiet
    7,22,37,52 * * * * root /path/to/pmb.py backup -i --quiet
