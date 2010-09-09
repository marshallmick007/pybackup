#!/usr/bin/python

import sys
import getopt
import os
import os.path
import datetime

try:
	import sqlite3
except ImportError:
	print "pysqlite not found, please download and install from http://code.google.com/p/pysqlite/"
	sys.exit(2)


VERBOSE = False
PYBACKUPPATH = ".pybackup"
CONFIGFILE = "pybackup.config"
DATADIR = "data"
DATABASEFILE="pybackup.db"
CURRENTVERSION = "0.1"
CONFIGVERSION=""

SQL_CREATEFILETABLE="CREATE TABLE IF NOT EXISTS files (filename TEXT, is_directory INTEGER, last_backup_hash TEXT, date_added TEXT, date_last_backed_up TEXT)"
SQL_LISTFILES="SELECT * FROM files ORDER BY filename"
SQL_ADDFILE='INSERT INTO files VALUES (?, ?, ?, ?, ?)'
SQL_CREATEFILETABLEINDEX="CREATE UNIQUE INDEX IF NOT EXISTS idx_FileName on files (filename)"
SQL_DELETEFILE='DELETE FROM files WHERE filename=?'

def main(argv):
	global VERBOSE
	
	try:
		opts, args = getopt.getopt(argv, "hvla:d:", ["help", "verbose", "configure", "add", "list", "delete"]) 
	except getopt.GetoptError, err: 
		print str(err)
		usage()
		sys.exit(2)
	addfiles = []
	deletefiles = []
	runconfiguration = False
	listfiles = False

	for o, a in opts:
		if o in ("-v","--verbose"):
			VERBOSE = True
		elif o == "--configure":
			runconfiguration = True
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-a", "--add"):
			addfiles.append(a)
		elif o in ("-d", "--delete"):
			deletefiles.append(a)
		elif o in ("-l", "--list"):
			listfiles = True
		else:
			#should be the file name
			assert False, "unhandled option"

	if runconfiguration:
		configure()
		sys.exit()
	elif not is_configured():
		print "pybackup is not configured. Run pybackup.py --configure"
		sys.exit(2)

	read_configuration()

	if listfiles:
		list_files()
	elif addfiles:
		process_adds( addfiles )
	elif deletefiles:
		process_deletes( deletefiles )
	else:
		usage()
		sys.exit(2)
		
		

def usage():
	print "Usage: pybackup.py [-h] [-v] file"
	
def read_configuration():
	return


def process_adds( files ):
	global VERBOSE
	for f in files:
		fullpath = compute_full_path( f )
		if os.path.exists(fullpath):
			if os.path.isfile(fullpath):
				add_to_file_table( fullpath, False )
			elif os.path.isdir(fullpath):
				add_to_file_table( fullpath, True )
				
		elif VERBOSE:
			print "Cannot add file " + f + ", it does not exist."

def process_deletes( files ):
	global VERBOSE
	for f in files:
		fullpath = compute_full_path( f )
		delete_file_from_table( fullpath )

def compute_full_path(file):
	#pwd = os.getcwd()
	#basepath = os.path.basename(file)
	return os.path.abspath(file)
	#if basepath == '':
	#	return os.path.join(pwd, file)
	#elif os.path.isabs(file):
	#	return file
	#elif file.startswith("~"):
	#	return os.path.expanduser(file)
	#else:
	#	return os.path.relpath(file)

def configure():
	global VERBOSE
	config = config_file_name()
	data = data_directory()
	print "pybackup configuration wizard:"
	print "pybackup settings will be located at " + config
	print "pybackup data will be located in " + data
	
	try:
		if not os.path.exists( data ):
			if VERBOSE:
				print "\tCreating directory " + data
			os.makedirs( data )
	except OSError, err:
		print "Error while configuring pybackup"
		print str(err)
		sys.exit(2)
	try:	
		if not os.path.exists( config ):
			if VERBOSE:
				print "\tCreating file " + config
			FILE = open(config, "w")
			FILE.writelines( generate_config_header() )
			FILE.close()
	except OSError, err:
		print "Error while configuring pybackup"
		print str(err)
		sys.exit(2)	
	
	try:
		datafile = get_database_name()
		conn = sqlite3.connect(datafile)
		c = conn.cursor()
		
		c.execute(SQL_CREATEFILETABLE)

		c.close()
		
	except sqlite3.Error, err:
		print "sqlite3 error: {0}".format(str(err))
		sys.exit(2)


def add_to_file_table(filename, is_directory):
	global VERBOSE
	global SQL_ADDFILE
	try:
		datafile = get_database_name()
		conn = sqlite3.connect(datafile)

		c = conn.cursor()
		print 'adding file {0}'.format(filename)
		
		args = []
		args.append(filename)
		args.append(1 if is_directory else 0)
		args.append(None)
		args.append(str(datetime.datetime.now()))
		args.append(None)
		
		c.execute(SQL_ADDFILE, args)
		if VERBOSE:
			print "Inserted row {0}".format(c.lastrowid)
		
		if c.lastrowid < 1:
			print "Unknown error while adding file to the database. No RowId was generated"
			
		conn.commit()
		c.close()
		conn.close()
		
	except sqlite3.Error, err:
		if VERBOSE:
			print "\t[sqlite3] error: {0}".format(str(err))
		print "WARNING: Unable to add file {0} to backup".format(filename)

def delete_file_from_table(file):
	global VERBOSE
	global SQL_DELETEFILE
	try:
		datafile = get_database_name()
		conn = sqlite3.connect(datafile)
		filename = compute_full_path(file)
		c = conn.cursor()
		print 'Removing file {0} from backup set'.format(filename)
		
		args = []
		args.append(filename)
		
		c.execute(SQL_DELETEFILE, args)

		conn.commit()
		c.close()
		conn.close()
		
	except sqlite3.Error, err:
		if VERBOSE:
			print "\t[sqlite3] error: {0}".format(str(err))
		print "WARNING: Unable to delete file {0} from backup set".format(filename)
	


def list_files():
	global VERBOSE
	global SQL_LISTFILES
	try:
		print "Files configured for backup:"
		datafile = get_database_name()
		conn = sqlite3.connect(datafile)

		c = conn.cursor()

		c.execute(SQL_LISTFILES)

		for row in c:
			print row[0]

		c.close()
		conn.close()
		
	except sqlite3.Error, err:
		if VERBOSE:
			print "\t[sqlite3] error: {0}".format(str(err))
		print "WARNING: Unable to list files."

def migrate():
	global CONFIGVERSION

def readconf():
	print "TODO: Read configuration file"

def is_configured():
	global VERBOSE
	p = config_file_name()
	
	if os.path.exists( p ):
		return True
	else:
		if VERBOSE:
			print "Unable to find pybackup configuration " + p
		return False
	
def config_file_name():
	global PYBACKUPPATH
	global CONFIGFILE
	homedir = os.getenv("HOME")
	p = os.path.join( homedir, PYBACKUPPATH )
	p = os.path.join( p, CONFIGFILE )
	
	return p

def data_directory():
	global PYBACKUPPATH
	global CONFIGFILE
	homedir = home_directory()
	p = os.path.join( homedir, PYBACKUPPATH )
	p = os.path.join( p, DATADIR )

	return p

def get_database_name():
	global DATABASEFILE
	return os.path.join( data_directory(), DATABASEFILE )

def home_directory():
	return os.getenv("HOME")

def generate_config_header():
	global CURRENTVERSION
	lines = ["# pybackup autogenerated configuration file {0}\n".format(str(datetime.datetime.now())),
	 		 "# Version: {0}\n".format(CURRENTVERSION) ]
	return lines


if __name__ == "__main__":
	main(sys.argv[1:])