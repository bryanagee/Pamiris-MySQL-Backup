#!/usr/bin/python

# Author: Kyle Terry (Pamiric Inc)
# Date: 02/22/2010

import os, sys, time, commands
import getopt, ConfigParser
import logging
import logging.config
import glob
from subprocess import call
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
        config.read('config.cfg')

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
                options, args = getopt.gnu_getopt(sys.argv[1:], 'fi', ['full', 'incremental', 'time=', 'quiet'])
        except getopt.GetoptError, err:
                logger.error(err)
                print str(err)
                sys.exit(2)

        # check for the quiet option
        for o, a in options:
                if '--quiet' == o:
                        logger.info('Entering quiet mode...')
                        quiet = True
                        break

        logger.info('### Starting Pamiris MySQL Backup Application ###')
        if not quiet:
                print 'Starting Pamiris MySQL Backup Application'

        # log what was passed in via command line
        logger.info('Running with: %s' % (' '.join(sys.argv[1:])))

        # we do this because we only want either backup, restore, or fetch to be passed in. aka one at a time
        if len(args) > 1:
                logger.error('Cannot pass in more than one action (argument)')
                if not quiet:
                    print 'FATAL: Cannot pass in more than one action (argument)'
                sys.exit(2)

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
                logger.error("Argument '%s' not recognized" % (args[0]))
                if not quiet:
                    print "FATAL: Argument '%s' not recognized" % (args[0])
                sys.exit(2)

def backup():
        """backup method for running full and incremental mysql backups"""
        backup_option_found = False
        for o, a in options:
                if o in ('-f', '--full'):
                        backup_option_found = True
                        logger.info('Running full backup...')
                        if not quiet:
                                print 'Running full backup...'

                        # run the full backup method
                        _backup_full()
                        break
                elif o in('-i', '--incremental'):
                        backup_option_found = True
                        logger.info('Running incremental backup...')
                        if not quiet:
                                print 'Running incremental backup...'

                        # run the incremental backup method
                        _backup_incremental()
                        break

        if not backup_option_found:
                logger.error('No backup option was passed in. Full or incremental flag is required. Backup terminating...')
                if not quiet:
                        print 'FATAL: No backup option was passed in. Full or incremental flag is required. Backup terminating...'
                sys.exit(2)

def _backup_full():
        """full backup"""
        logger.info('Full backup in progress...')
        if not quiet:
                print 'Full backup in progress...'
        
        # check if the full directory is set in config file and attempt to enter it
        full_path = config.get('Backup', 'full_path')
        if not full_path:
                logger.error('`full_path` is required to be set in config.cfg. Backup terminiating...')
                if not quiet:
                        print 'FATAL: `full_path` is required to be set in config.cfg. Backup terminiating...' 
                sys.exit(2)

        try:
                os.chdir(full_path)
                logger.info('Changing directories... (%s)' % (full_path))
        except OSError as err:
                logger.error(err)
                if not quiet:
                        print 'Directory does not exist. Backup terminating...'
                sys.exit(2)

        # setup the date and time and prepare to check for the existence of an already-run full backup
        now = datetime.today()
        date = now.strftime('%Y%m%d')
        dateandtime = now.strftime('%Y%m%d_%H%M')
        file_prefix = config.get('Backup', 'file_prefix')
        if not file_prefix:
                logger.error('No backup file prefix was set in the config.cfg. Backup terminating...')
                if not quiet:
                        print 'FATAL: No backup file prefix was set in the config.cfg. Backup terminating...'
                sys.exit(2)

        test_file = '%sfull_%s*' % (file_prefix, date)
        
        # check if a full backup has been run for today
        if glob.glob(test_file):
                logger.error('Full backup for today already exists. Backup terminating...')
                if not quiet:
                        print 'Full backup for today already exists. Backup terminating...'
                sys.exit(2)
        
        # prepare to run the full back up
        file_name = '%sfull_%s.sql' % (file_prefix, dateandtime)

        logger.info('Preparing binary logs...')
        if not quiet:
                print 'Preparing binary logs...'

        # this checks the bin-log directory and puts the output into a file.
        # we do this because we need to keep track of the last bin-log
        # available before we flush them with --flush-logs.
        # this will let us easily grab the bin-logs created after a full backup
        # to be used with incremental backups.
        ls_command = "ls -l %s |grep %s.0 | awk '{print $8}' > bin_logs" % (config.get('Backup', 'bin_log_path'),
                config.get('Backup', 'bin_log_name'))
        os.system(ls_command)
        last_line = file('bin_logs', "r").readlines()[-1]
        os.system('rm -f bin_log_info')
        os.system('touch bin_log_info')

        file('bin_log_info', 'w').write('before:' + last_line)

        logger.info('Running mysqldump and creating File: %s' % (file_name))
        if not quiet:
                print 'Running mysqldump and Creating File: %s' % (file_name)

        backup_command = 'mysqldump --flush-logs -u%s --password=%s %s > %s' % (config.get('Backup', 'username'),
                config.get('Backup', 'password'),
                config.get('Backup', 'database'),
                file_name)
        os.system(backup_command)

        logger.info('Full backup created successfully!')
        if not quiet:
                print 'Full backup created successfully!'

        logger.info('Compressing backup...')
        if not quiet:
                print 'Compressing backup...'

        compress_command = 'gzip %s' % (file_name)
        os.system(compress_command)

        logger.info('File compression completed...')
        if not quiet:
                print 'File compression completed...'
        
def _backup_incremental():
        """incremental backup"""
        logger.info('Incremental backup in progress...')
        if not quiet:
                print 'Incremental backup in progress...'
        
        # prepare the date and file naming
        now = datetime.today()
        date = now.strftime('%Y%m%d')
        dateandtime = now.strftime('%Y%m%d_%H%M')
        
        file_prefix = config.get('Backup', 'file_prefix')
        if not file_prefix:
                logger.error('No backup file prefix was set in the config.cfg. Backup terminating...')
                if not quiet:
                        print 'FATAL: No backup file prefix was set in the config.cfg. Backup terminating...'
                sys.exit(2)
        
        # relist binary logs
        os.system('rm bin_logs')
        ls_command = "ls -l %s |grep %s.0 | awk '{print $8}' > bin_logs" % (config.get('Backup', 'bin_log_path'),
                config.get('Backup', 'bin_log_name'))
        os.system(ls_command)
        last_line = file('bin_logs', "r").readlines()[-1]
        
        # add end to bin_log_info
        file('bin_log_info', 'a').write('last:' + last_line)

        # check last binary log before the last full flush. +1 from that is the start of the bin log range
        full_path = config.get('Backup', 'full_path')
        if not full_path:
                logger.error('`full_path` is required to be set in config.cfg. Backup terminiating...')
                if not quiet:
                        print 'FATAL: `full_path` is required to be set in config.cfg. Backup terminiating...' 
                sys.exit(2)
        
        try:
                os.chdir(full_path)
                logger.info('Changing directories... (%s)' % (full_path))
        except OSError as err:
                logger.error(err)
                if not quiet:
                        print 'Directory does not exist. Backup terminating...'
                sys.exit(2)

        bin_log_info = file('bin_log_info', 'r').readlines()

        bin_log_hash = {}
        for line in bin_log_info:
                t_line = line.split(':')
                bin_log_hash[t_line[0]] = t_line[1]

        log_tracker = {}
        log_tracker['start'] = int(bin_log_hash['before'].split('.')[1]) + 1


        


def restore():
        """restore method"""
        print "restoring..."

def fetch():
        """fetch method"""
        print "fetching backup..."

main()
