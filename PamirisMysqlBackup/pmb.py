#!/usr/bin/python

# Author: Kyle Terry (Pamiric Inc)
# Date: 02/22/2010

import os, sys, time, commands
import getopt, ConfigParser
import logging
import logging.config
import glob
import subprocess
import shlex

from datetime import datetime

def main():
    """main method for parsing the command line options and what happens after that"""

    # options, args and config all need to be global so they can be used in other methods
    global options, args, config, logger, quiet

    # quiet tells the application to not print it's current status to stdout. It just
    # logs instead.
    quiet = False

    # open the config file and parse it
    config = ConfigParser.RawConfigParser()
    config_file = '%s/config.cfg' % (os.path.abspath(os.path.dirname(__file__)))
    config.read(config_file)

    # configure the logger
    logger = logging.getLogger("PMB LOG")
    logger.setLevel(logging.DEBUG)
    fileHandler = logging.FileHandler(config.get('Logging', 'log_path'))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    # list of available options
    available = ['backup', 'restore', 'fetch']

    # attemped to parse the command line arguments. getopt will detect and throw an exception if an argument
    # exists that wasn't meant to be there.
    try:
        options, args = getopt.gnu_getopt(\
                sys.argv[1:], 
                'fi', 
                ['full',
                 'incremental',
                 'all-databases',
                 'database=',
                 'time=',
                 'date=',
                 'quiet']
        )
    except getopt.GetoptError, err:
        logAndPrint(err, 'error', True, True)

    # check for the quiet option
    for o, a in options:
        if '--quiet' == o:
            logger.info('Entering quiet mode...')
            quiet = True
            break

    message = '### Starting Pamiris MySQL Backup Application ###'
    logAndPrint(message, 'info')

    # log what was passed in via command line
    logAndPrint('Running with: %s' % (' '.join(sys.argv[1:])), 'info', False)

    # we do this because we only want either backup, restore, or fetch to be passed in. aka one at a time
    if len(args) > 1:
        message = 'FATAL: Cannot pass in more than one action (argument)'
        logAndPrint(message, 'error', True, True)

    # detect which action is needed and call it's method
    if 'backup' == args[0]:
        logger.info('Database backup wanted...')
        backup()
    elif 'restore' == args[0]:
        logger.info('Database restore wanted...')
        restore()
    elif 'fetch' == args[0]:
        logger.info('Database fetch wanted...')
        fetch()
    else:
        message = "FATAL: Argument '%s' not recognized" % (args[0])
        logAndPrint(message, 'error', True, True)

def backup():
    """backup method for running full and incremental mysql backups"""
    backup_option_found = False
    for o, a in options:
        if o in ('-f', '--full'):
            backup_option_found = True
            message = 'Running full backup...'
            logAndPrint(message, 'info')

            # run the full backup method
            _backup_full()
            break
        elif o in('-i', '--incremental'):
            backup_option_found = True
            message = 'Running incremental backup...'
            logAndPrint(message, 'info')

            # run the incremental backup method
            _backup_incremental()
            break

    if not backup_option_found:
        message = 'FATAL: No backup option was passed in. Full or incremental flag is required. Backup terminating...'
        logAndPrint(message, 'error', True, True)

def _backup_full():
    """full backup"""
    message = 'Full backup in progress...'
    logAndPrint(message, 'info')
    
    # check if the full directory is set in config file and attempt to enter it
    full_path = config.get('Backup', 'full_path')
    if not full_path:
        message = '`full_path` is required to be set in config.cfg. Backup terminiating...'
        logAndPrint(message, 'error', True, True)

    try:
        os.chdir(full_path)
        logger.info('Changing directories... (%s)' % (full_path))
    except Exception, e:
        message = 'Directory does not exist. Backup terminating...'
        logAndPrint(message, 'error', True, True)

    # setup the date and time and prepare to check for the existence of an already-run full backup
    now = datetime.today()
    date = now.strftime('%Y%m%d')
    dateandtime = now.strftime('%Y%m%d_%H%M')
    file_prefix = config.get('Backup', 'file_prefix')
    if not file_prefix:
        message = 'FATAL: No backup file prefix was set in the config.cfg. Backup terminating...'
        logAndPrint(message, 'error', True, True)

    test_file = '%sfull_%s*' % (file_prefix, date)
    
    # check if a full backup has been run for today
    if glob.glob(test_file):
        message = 'Full backup for today already exists. Backup terminating...'
        logAndPrint(message, 'error', True, True)
    
    # prepare to run the full back up
    file_name = '%sfull_%s' % (file_prefix, dateandtime)

    logAndPrint('Preparing binary logs...', 'info')

    # this checks the bin-log directory and puts the output into a file.
    # we do this because we need to keep track of the last bin-log
    # available before we flush them with --flush-logs.
    # this will let us easily grab the bin-logs created after a full backup
    # to be used with incremental backups.
    ls_command = "ls -l --time-style=long-iso %s |grep %s.0 | awk '{print $8}' > bin_logs" \
            % (config.get('Backup', 'bin_log_path'),
               config.get('Backup', 'bin_log_name'))
    os.system(ls_command)
    last_line = file('bin_logs', "r").readlines()[-1]
    os.system('rm -f bin_log_info')
    os.system('touch bin_log_info')

    file('bin_log_info', 'w').write('before:' + last_line)

    message = 'Running mysqldump and creating File: %s' % (file_name)
    logAndPrint(message, 'info')

    try:
        if config.get('Backup', 'db_host') is not None:
            db_host = '-h %s' % config.get('Backup', 'db_host')
    except:
        db_host = '-h localhost'

    database = config.get('Backup', 'database')
    for o,a in options:
        if '--all-databases' == o:
            database = o
            break
        elif '--database' == o:
            database = a
            break

    backup_command = 'mysqldump %s --add-drop-database --flush-logs -u%s %s --password=%s --result-file=%s.sql' % \
        (database, #config.get('Backup', 'database'),
        config.get('Backup', 'username'),
        db_host,
        config.get('Backup', 'password'),
        file_name)
    #os.system(backup_command)
    backup_command = shlex.split(backup_command)
    process = subprocess.Popen(backup_command, stderr=subprocess.PIPE)
    
    # Set the stderr (if any) in p_out
    p_out = process.communicate()

    # Check for erorrs in the output. Error will not be None and 
    # will be longer than ''
    # 
    # we completely ignore stdout since it's being piped into
    # a file using the mysqldump --result-file flag.
    for v in p_out:
        if v is not None and v != '':
            os.system('rm -f %s.sql' % (file_name))
            message = 'Backup encountered a fatal error from MySQL. Exiting...'
            logAndPrint(message, 'error')
            logAndPrint(v, 'error', exit=True)

    message = 'Full backup created successfully!'
    logAndPrint(message, 'info')

    if config.get('Encryption', 'enabled')is not None and \
    config.get('Encryption', 'enabled') == 'true':

        logAndPrint('Encryption enabled...', 'info')
        logAndPrint('Encrypting and compressing backup...', 'info')

        encryption_command = 'gpg --always-trust -r %s --output %s.gpg --encrypt %s.sql' \
                % (config.get('Encryption', 'key_name'), file_name, file_name)
        logAndPrint('running: %s' % encryption_command, 'info')

        encryption_command = shlex.split(encryption_command)
        process = subprocess.Popen(encryption_command, stderr=subprocess.PIPE)
        p_out = process.communicate()

        # Check for erorrs in the output. Error will not be None and 
        # will be longer than ''
        # 
        # we completely ignore stdout since it's being piped into
        # a file using the mysqldump --result-file flag.
        for v in p_out:
            if v is not None and v != '':
                os.system('rm -f %s.sql' % (file_name))
                message = 'Backup encountered a fatal error when encrypting with GPG. Exiting...'
                logAndPrint(message, 'error')
                logAndPrint(v, 'error', exit=True)

        #os.system(encryption_command)
        logAndPrint('removing unencrypted sql file...(%s.sql)' \
                % (file_name), 'info')
        os.system('rm -rf %s.sql' % file_name)

    else:
        logAndPrint('Compressing backup...', 'info')

        compress_command = 'gzip %s' % (file_name)
        os.system(compress_command)

        logAndPrint('File compression completed...', 'info')

    
def _backup_incremental():
    """incremental backup"""
    logAndPrint('Incremental backup in progress...', 'info')
    
    # prepare the date and file naming
    now = datetime.today()
    date = now.strftime('%Y%m%d')
    dateandtime = now.strftime('%Y%m%d_%H%M')
    
    file_prefix = config.get('Backup', 'file_prefix')
    if not file_prefix:
        message = 'FATAL: No backup file prefix was set in the config.cfg. Backup terminating...'
        logAndPrint(message, 'error', True, True)

    # check last binary log before the last full flush. +1 from that is the start of the bin log range
    full_path = config.get('Backup', 'full_path')
    if not full_path:
        message = '`full_path` is required to be set in config.cfg. Backup terminiating...'
        logAndPrint(message, 'error', True, True)
    
    try:
        os.chdir(full_path)
        logger.info('Changing directories... (%s)' % (full_path))
    except Exception, e:
        message = 'Directory does not exist. Backup terminating...'
        logAndPrint(message, 'error', True, True)

    # check for the existence of a full backup for today
    full_backup_test = '%sfull_%s*' % (file_prefix, date)
    if not glob.glob(full_backup_test):
        message = 'There was no full backup run for today. Run the application with --full, then run incrementals after that'
        logAndPrint(message, 'error', True, True)

    # relist binary logs
    os.system('rm -rf bin_logs')
    ls_command = "ls -l --time-style=long-iso %s |grep %s.0 | awk '{print $8}' > bin_logs" % (config.get('Backup', 'bin_log_path'),
        config.get('Backup', 'bin_log_name'))
    os.system(ls_command)
    last_line = file('bin_logs', "r").readlines()[-1]
    
    # add end to bin_log_info
    file('bin_log_info', 'a').write('last:' + last_line)

    bin_log_info = file('bin_log_info', 'r').readlines()

    bin_log_hash = {}
    for line in bin_log_info:
        t_line = line.split(':')
        bin_log_hash[t_line[0]] = t_line[1]

    log_tracker = {"first": int(bin_log_hash['before'].split('.')[1]) + 1, "last": int(bin_log_hash['last'].split('.')[1])}
    log_tracker['first'] = int(bin_log_hash['before'].split('.')[1]) + 1
    log_tracker['last'] = int(bin_log_hash['last'].split('.')[1])

    logAndPrint('Found beginning and end of the binary logs...', 'info')
    logAndPrint(log_tracker, 'info')

    #flush the logs, update bin_log_info and copy the binary log files over
    logAndPrint('Flushing binary logs...', 'info')
    os.system('mysql %s -u%s --password=%s -e "flush logs;"' %
        (config.get('Backup', 'database'),config.get('Backup', 'username'),config.get('Backup', 'password')))

    os.system('rm -rf bin_log_info')
    file('bin_log_info', 'w').write('before:%s' % (last_line))

    # change directories
    if config.get('Backup', 'inc_path'):
        try:
            os.chdir(config.get('Backup', 'inc_path'))
            logger.info('Changing directories... (%s)' % (config.get('Backup', 'inc_path')))
        except Exception, e:
            logAndPrint('Directory does not exist. Backup terminating...', 'error', True, True)
    else:
        logger.info('No inc path found in config.cfg. Staying in current directory (%s)...' % (config.get('Backup', 'full_path')))


    # start copying
    logger.info('Copying binary logs...')
    if not quiet:
        print 'Copying binary logs...'

    import pickle

    ignore_file = '%signore_logs' % (config.get('Backup', 'full_path'))

    ignore_list = None
    try:
        f = open(ignore_file, 'rb')
        ignore_list = pickle.load(f)
        f.close()
    except:
        pass

    os.system('rm -rf %s' % (ignore_file))

    for i in range(int(log_tracker['first']), int(log_tracker['last']) + 1):
        log_file = '%s.%06d' % (config.get('Backup', 'bin_log_name'), i)
        test_file = '%s%s' % (config.get('Backup', 'bin_log_path'), log_file)
        if ignore_list is not None and test_file in ignore_list:
            continue
        os.system('cp %s%s .' % 
            (config.get('Backup', 'bin_log_path'), log_file))
        logAndPrint(log_file, 'info')

    # check for the existence of --database or --all-databases options
    # and parse which database will be pulled out of the bin log
    database = '--database=%s' % (config.get('Backup', 'database'))
    for o,a in options:
        if '--all-databases' == o:
            database = ''
            break
        elif '--database' == o:
            database = '--database=%s' % (a)
            break

    # convert to sql and compress
    bam_a_list = []
    logAndPrint('Converting binary logs to SQL...', 'info')
    for i in range(int(log_tracker['first']), int(log_tracker['last']) + 1):
        bam_a_list.append('%s.%06d' % (config.get('Backup', 'bin_log_name'), i))
    file_name = '%sinc_%s' % (file_prefix, dateandtime)

    convert_to_sql = 'mysqlbinlog %s %s > %s.sql' % \
            (database, 
             ' '.join(bam_a_list),
             file_name)
    os.system(convert_to_sql)

    logAndPrint('Removing converted bin logs', 'info')
    for log in bam_a_list:
        os.system('rm -rf %s' % (log))

    logAndPrint('Converted successfully!', 'info')

    if config.get('Encryption', 'enabled') == 'true':
        # encrypt
        logAndPrint('Encryption enabled...', 'info')
        logAndPrint('Encrypting and compressing backup...', 'info')

        encryption_command = 'gpg --always-trust -r %s --output %s.gpg --encrypt %s.sql' % \
                (config.get('Encryption', 'key_name'), file_name, file_name)
        logAndPrint('running: %s' % encryption_command, 'info')
        encryption_command = shlex.split(encryption_command)
        process = subprocess.Popen(encryption_command, stderr=subprocess.PIPE)
        p_out = process.communicate()
        #os.system(encryption_command)
        for v in p_out:
            if v is not None and v != '':
                os.system('rm -f %s.sql' % (file_name))
                message = 'Backup encountered a fatal error when encrypting with GPG. Exiting...'
                logAndPrint(message, 'error')
                logAndPrint(v, 'error', exit=True)

        logAndPrint('removing unencrypted sql file...(%s.sql)' % (file_name), 'info')
        os.system('rm -rf %s.sql' % file_name)
    else:
        # start compressing...
        logAndPrint('Compressing incremental backup...', 'info')

        compress_command = 'gzip %s' % (file_name)
        os.system(compress_command)

        logAndPrint('Compressing backup completed successfully!', 'info')
    
def restore():
    """restore method"""
    logAndPrint('Restore needs confirmation...', 'info')
    print "WARNING: You have chosen to restore the database. This will destroy your current instance and replace it with the given back up."
    answer = raw_input('Are you sure? yes/no ')
    logAndPrint('"%s" given as the answer' % (answer), 'info', False)
    if answer != 'yes':
        logAndPrint('User wanted to abort database restore. Program exiting...', 'info', True, True)

    """for o,a in options:

    if not '--time' in options or not '--date' in options:
        logger.error('The restore option needs --time= and --date flags. Restore terminating...')
        if not quiet:
            print 'The restore option needs --time= and --date flags. Restore terminating...'

        sys.exit(2)
    logger.info('Restoring database...')"""

    extension = '.sql.gz'
    if config.get('Encryption', 'enabled') == 'true':
        extension = '.gpg'

    for o,a in options:
        if '--time' in o:
            _time = a

    for o,a in options:
        if '--date' in o:
            _date = a

    #_time = options['time']
    #_date = options['date']

    # change directories

    try:
        os.chdir(config.get('Backup', 'full_path'))
        logAndPrint('Changing directories... (%s)' % (config.get('Backup', 'full_path')), 'info')
    except Exception, e:
        logAndPrint('FATAL: Directory does not exist. Backup terminating...', 'error', True, True)

    full_backup = glob.glob('%sfull_%s*' % (config.get('Backup', 'file_prefix'), str(_date)))

    if len(full_backup) < 1:
        message = 'FATAL: Looks like there was no full backup run for this date. Restore terminating...'
        logAndPrint(message, 'error', True, True)

    if len(full_backup) > 1:
        message = 'FATAL: Looks like there is more than one full backup for this date in %s.' % \
            (config.get('Backup', 'full_path'))
        logAndPrint(message, 'error', True, True)

    full_output = full_backup[0].split('.')[0] + '.sql'

    decrypt = 'gpg --quiet --passphrase-file %s --output %s%s --decrypt %s' % \
        (config.get('Encryption', 'passphrase_file'),
         config.get('Main', 'tmp'),
         full_output,
         full_backup[0])

    decrypt = shlex.split(decrypt)
    
    logAndPrint('Decrypting full and incremental backup files...','info')
    #os.system(decrypt)
    process = subprocess.Popen(decrypt, stderr=subprocess.PIPE)
    p_out = process.communicate()

    for v in p_out:
        if v is not None and v != '':
            logAndPrint('Backup encountered a fatal error when dencrypting with GPG. Exiting...', 'error')
            logAndPrint(v, 'error', exit=True)

    if config.get('Backup', 'inc_path'):
        try:
            os.chdir(config.get('Backup', 'inc_path'))
            logAndPrint('Changing directories... (%s)' % (config.get('Backup', 'inc_path')), 'info')
        except Exception, e:
            logAndPrint('FATAL: Directory does not exist. Backup terminating...', 'error', True, True)

    inc_back_name = '%sinc_%s_' % (config.get('Backup', 'file_prefix'), str(_date))

    # glob for inc files
    inc_backs = glob.glob(inc_back_name + '*')

    decrypt_incs = []

    # loop for a crap ton and check if the file exists in the list
    # there is most likely a more efficient way of doing this...
    for inc in inc_backs:
        inc_time = inc.split('.')[0].split('_')[3]
        if inc_time <= _time:
            decrypt_incs.append(inc)

    decrypt_incs.sort()
    
    inc_outputs = []
    for inc in decrypt_incs:
        out = config.get('Main', 'tmp') + inc.split('.')[0] + '.sql'
        decrypt = 'gpg --passphrase-file %s --output %s --decrypt %s' % \
            (config.get('Encryption', 'passphrase_file'),
             out,
             inc)
        os.system(decrypt)
        inc_outputs.append(out)

    build_me = 'cat %s%s %s > %stemp_restore.sql' % ( \
            config.get('Main', 'tmp'),
            full_output,
            ' '.join(inc_outputs),
            config.get('Main', 'tmp'))
    os.system(build_me)

    database = config.get('Backup', 'database')
    for o,a in options:
        if '--all-databases' == o:
            database = ''
            break
        elif '--database' == o:
            database = '--database=%s' % (a)
            break

    start_flush = 'mysql --password=%s -e "FLUSH LOGS;"' % \
            (config.get('Backup', 'password'))
    start_flush = shlex.split(start_flush)

    process = subprocess.Popen(start_flush, stdout=subprocess.PIPE)
    p_out = process.communicate()

    for v in p_out:
        if v is not None and v !='':
            message = 'Backup encountered an error when accessing MySQL. Backup Exiting...'
            logAndPrint(message, 'error')
            logAndPrint(v, 'error', exit=True)

    #os.system(start_flush)

    ls_command = "ls -l --time-style=long-iso %s |grep %s.0 | awk '{print $8}' > ignore_bin_log" % \
        (config.get('Backup', 'bin_log_path'),
        config.get('Backup', 'bin_log_name'))
    os.system(ls_command)
    first_log = file('ignore_bin_log', "r").readlines()[-1]
    
    """
    logAndPrint('Dropping database...', 'info')
    drop_db_command = 'mysql --password=%s -e "drop database %s"' % \
            (config.get('Backup', 'password'),
             config.get('Backup', 'database'))
    drop_db_command = shlex.split(drop_db_command)
    
    process = subprocess.Popen(drop_db_command, stderr=subprocess.PIPE)
    p_out = process.communicate()
    
    for v in p_out:
        if v is not None and v != '':
            message = 'Backup encountered an error when accessing MySQL. Backup Exiting...'
            logAndPrint(message, 'error')
            logAndPrint(v, 'error', exit=True)
    #os.system(drop_db_command)
    """

    """
    logAndPrint('Restoring database from backup...', 'info')
    create_db_command = 'mysql --password=%s -e "create database %s"' % \
            (config.get('Backup', 'password'),
             config.get('Backup', 'database'))
    create_db_command = shlex.split(create_db_command)

    process = subprocess.Popen(create_db_command, stderr=subprocess.PIPE)
    p_out = process.communicate()

    for v in p_out:
        message = 'Backup encountered an error when accessing MySQL. Backup Exiting...'
        logAndPrint(message, 'error')
        logAndPrint(v, 'error', exit=True)
    #os.system(create_db_command)
    """

    logAndPrint('Restoring database from backup...', 'info')

    cat_full_command = 'cat %stemp_restore.sql' % (config.get('Main', 'tmp'))
    cat_full_command = shlex.split(cat_full_command)
    
    # The output of this process will be piped into the restore process below.
    # This is done because subprocess separates the processes and how
    # piping actually works.
    cat_process = subprocess.Popen(cat_full_command, stdout=subprocess.PIPE)

    restore_command = 'mysql %s --password=%s' % \
            (database, #config.get('Backup', 'database'),
             config.get('Backup', 'password'))
    restore_command = shlex.split(restore_command)

    process = subprocess.Popen(restore_command, stdin=cat_process.stdout, stderr=subprocess.PIPE)
    p_out = process.communicate()

    for v in p_out:
        if v is not None and v != '':
            message = 'Backup encountered an error when accessing MySQL. Backup Exiting...'
            logAndPrint(message, 'error')
            logAndPrint(v, 'error', exit=True)

    #os.system(restore_command)

    ls_command = "ls -l --time-style=long-iso %s |grep %s.0 | awk '{print $8}' > ignore_bin_log" % \
        (config.get('Backup', 'bin_log_path'),
        config.get('Backup', 'bin_log_name'))
    os.system(ls_command)
    last_log = file('ignore_bin_log', "r").readlines()[-1]

    end_flush = 'mysql --password=%s -e "FLUSH LOGS;"' % \
            (config.get('Backup', 'password'))
    end_flush = shlex.split(end_flush)

    process = subprocess.Popen(end_flush, stderr=subprocess.PIPE)
    p_out = process.communicate()

    for v in p_out:
        if v is not None and v != '':
            message = 'Backup encountered an error when accessing MySQL. Backup Exiting...'
            logAndPrint(message, 'error')
            logAndPrint(v, 'error', exit=True)

    #os.system(end_flush)

    first_log = first_log.split('.')[1]
    last_log = last_log.split('.')[1]

    logs = []
    for i in range(int(first_log), int(last_log) + 1):
        logs.append('%s%s.%06d' % \
                (config.get('Backup', 'bin_log_path'),
                config.get('Backup', 'bin_log_name'),
                i))

    try:
        os.chdir(config.get('Backup', 'full_path'))
    except:
        logAndPrint('Director does not exist', 'error', True, True)
    
    import pickle

    try:
        f = file('ignore_logs', 'rb')
        f_logs = pickle.load(f)
        os.remove('ignore_logs')
        for log in f_logs:
            logs.append(log)
        f = file('ignore_logs', 'wb')
        pickle.dump(logs, f)
        f.close()
    except IOError:
        f = file('ignore_logs', 'wb')
        pickle.dump(logs, f)
        f.close()

    try:
        os.chdir(config.get('Backup', 'inc_path'))
    except:
        logAndPrint('Directory does not exist', 'error', True, True)

    logAndPrint('Cleaning up temp files...', 'info')

    cleanup_command = 'rm -f %s%s %s' % \
            (config.get('Main', 'tmp'),
             full_output,
             ' '.join(inc_outputs))

    os.system(cleanup_command)

    logAndPrint('Restore completed!', 'info')

def fetch():
    """fetch method"""
    logAndPrint('Fetching database backup from remote server...')
    for o,a in options:
        if o in ('--time'):
            _time = a
            time_found = True
            break

    for o,a in options:
        if o in ('--date'):
            _date = a
            date_found = True
            break

    if not time_found or not date_found:
        message = '--date and --time flags are required when trying to fetch a database backup'
        logAndPrint(message, 'error', True, True)

    full_file = '%sfull_%s_*' % (config.get('Backup', 'file_prefix'), _date)
    inc_file = '%sinc_%s_*' % (config.get('Backup', 'file_prefix'), _date)

    full_list_command = 'ssh %s "ls -l --time-style=long-iso %s | grep %s | awk \'{print $8}\'"' % \
            (config.get('Fetch', 'connection_string'),
             config.get('Fetch', 'remote_full_path'),
             full_file)
    f = os.popen(full_list_command)
    full_backup = [l.strip('\n').split(' ')[7] for l in f]
    print full_backup

    if len(full_backup) < 1:
        message = 'It doesn\'t look like the remote system has a full backup for the given date (%s)' % \
                (_date)
        logAndPrint(message, 'error', True, True)

    if len(full_backup) > 1:
        message = 'It looks like there is more than one backup for the given date (%s)' % (_date)
        logAndPrint(message, 'error', True, True)
 
    full_backup = full_backup[0]

    inc_list_command = 'ssh %s "ls -l --time-style=long-iso %s | grep %s | awk \'{print $8}\'"' % \
            (config.get('Fetch', 'connection_string'),
             config.get('Fetch', 'remote_inc_path'),
             inc_file)
    f = os.popen(inc_list_command)

    inc_backups = []
    later_backups = []
    for l in f.readlines():
        l = l.strip('\n').replace('  ', ' ').replace('  ', ' ')
        l = l.split(' ')[7]
        if int(l.split('.')[0].split('_')[3]) < int(_time):
            inc_backups.append(config.get('Fetch', 'remote_inc_path') + l)
            later_backups.append(l)

    scp_command = 'scp %s:"%s%s %s" %s' % (\
            config.get('Fetch', 'connection_string'),
            config.get('Fetch', 'remote_full_path'),
            full_backup,
            ' '.join(inc_backups),
            config.get('Main', 'tmp'))
    os.system(scp_command)

    # decompression or decryption
    if config.get('Encryption', 'enabled') == 'true':
        #decrypt
        tmp = config.get('Main', 'tmp')
        full_decrypt_command = 'gpg --passphrase-file %s --output %s --decrypt %s' % \
            (config.get('Encryption', 'passphrase_file'),
             tmp + full_backup.split('.')[0] + '.sql',
             tmp + full_backup)
        os.system(full_decrypt_command)

        later_backups.sort()
        for inc in later_backups:
            inc_decrypt_command = 'gpg --passphrase-file %s --output %s --decrypt %s' % \
                (config.get('Encryption', 'passphrase_file'),
                 tmp + inc.split('.')[0] + '.sql',
                 tmp + inc)
            os.system(inc_decrypt_command)

        cat_command = 'cat %s%s %s > %s%s_backup.sql' % \
            (config.get('Main', 'tmp'),
             full_backup.split('.')[0] + '.sql',
             [tmp + inc.split('.')[0] + '.sql' for inc in inc_backups],
             config.get('Fetch', 'local_save_path'),
             config.get('Backup', 'file_prefix'))

        os.system(cat_command)
    else:
        tmp = config.get('Main', 'tmp')
        full_decompress = 'gzip -d %s%s' % (tmp, full_backup)
        os.system(full_decompress)
        incs = []
        for inc in later_backups:
            inc_decompress = 'gzip -d %s%s' & (tmp, inc)
            incs = '%s%s' % (tmp, inc)
            os.system(inc_decompress)

        cat_command = 'cat %s%s %s > %s%s_backup.sql' % \
                (tmp,
                 full_backup.split('.'),
                 ' '.join([inc.strip('.gz') for inc in incs]),
                 config.get('Fetch', 'local_save_path'),
                 config.get('Backup', 'file_prefix'))

        os.system(cat_command)

def logAndPrint(message, type='info', print_message=True, exit=False):
    if type == 'info':
        logger.info(message)
    elif type == 'warn':
        logger.warn(message)
    elif type == 'error':
        logger.error(message)
    else:
        logger.info(message)

    if not quiet and print_message:
        print message

    if exit:
        sys.exit()

if __name__=='__main__':
    main()
