#!/usr/bin/python

import os, sys, time, commands
import getopt

def main():
        """main method for parsing the command line options and what happens after that"""

        global options, args

        # list of available options
        available = ['backup', 'restore', 'fetch']

        try:
                options, args = getopt.gnu_getopt(sys.argv[1:], 'fi', ['full', 'incremental', 'time='])
        except getopt.GetoptError, err:
                print str(err)
                sys.exit(2)

        if len(args) > 1:
                print 'FATAL: Cannot pass in more than one action (argument)'
                sys.exit(2)

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
