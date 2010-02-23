#!/usr/bin/python

# Author: Kyle Terry (Pamiric Inc)
# Date: 02/22/2010

import os, sys, time, commands
import getopt, ConfigParser
import logging
import logging.config

def main():
        """main method for parsing the command line options and what happens after that"""

        # options, args and config all need to be global so they can be used in other methods
        global options, args, config, logger, quiet

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
        logger.info('### Starting Pamiris MySQL Backup Application ###')

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

def _backup_incremental():
        """incremental backup"""
        logger.info('Incremental backup in progress...')
        if not quiet:
                print 'Incremental backup in progress...'

def restore():
        """restore method"""
        print "restoring..."

def fetch():
        """fetch method"""
        print "fetching backup..."

main()
