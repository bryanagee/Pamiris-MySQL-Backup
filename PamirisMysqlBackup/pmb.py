#!/usr/bin/python

# Author: Kyle Terry (Pamiric Inc)

import os, sys, time, commands
import getopt, ConfigParser
import logging
import logging.config

def main():
        """main method for parsing the command line options and what happens after that"""

        # options, args and config all need to be global so they can be used in other methods
        global options, args, config, logger

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
        logger.info('testing')


        # list of available options
        available = ['backup', 'restore', 'fetch']

        # attemped to parse the command line arguments. getopt will detect and throw an exception if an argument
        # exists that wasn't meant to be there.
        try:
                options, args = getopt.gnu_getopt(sys.argv[1:], 'fi', ['full', 'incremental', 'time='])
        except getopt.GetoptError, err:
                print str(err)
                sys.exit(2)

        # we do this because we only want either backup, restore, or fetch to be passed in. aka one at a time
        if len(args) > 1:
                print 'FATAL: Cannot pass in more than one action (argument)'
                sys.exit(2)

        # detect which action is needed and call it's method
        if 'backup' == args[0]:
                backup()
        elif 'restore' == args[0]:
                restore()
        elif 'fetch' == args[0]:
                fetch()
        else:
                print "FATAL: Argument '%s' not recognized" % (args[0])
                sys.exit(2)

def backup():
        """backup method for running full and incremental mysql backups"""
        print "Running backing up...\n"
        for o, a in options:
                if o in ('-f', '--full'):
                        _backup_full()
                elif o in('-i', '--incremental'):
                        _backup_incremental()


def _backup_full():
        """full backup"""
        print "Full backup in progress...\n"

def _backup_incremental():
        """incremental backup"""
        print "Incremental backup in progress...\n"

def restore():
        """restore method"""
        print "restoring..."

def fetch():
        """fetch method"""
        print "fetching backup..."

main()
