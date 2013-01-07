#-----------------------------------------------------------------------
# <copyright file="esedb_perf.py" company="Microsoft Corporation">
# Copyright (c) Microsoft Corporation.
# </copyright>
#-----------------------------------------------------------------------

import esedb
import System
import random

from System.Diagnostics import Stopwatch

database = 'wdbperf.db'

def insertRetrieveTest():
	n = 1000000
	db = esedb.open(database, 'n', True)
	data = '0123456789ABCDEF'
	timer = Stopwatch.StartNew()
	for i in xrange(n):
		db[i] = data
	timer.Stop()
	print 'Inserted %d records in %s' % (n, timer.Elapsed)
	(k,v) = db.first()
	timer = Stopwatch.StartNew()
	for i in xrange(n):
		data = db[k]
	timer.Stop()
	print 'Retrieved 1 record %d times in %s' % (n, timer.Elapsed)
	timer = Stopwatch.StartNew()
	i = 0
	for (k,v) in db:
		i += 1
	timer.Stop()
	print 'Scanned %d records in %s' % (i, timer.Elapsed)
	keys = db.keys()
	random.shuffle(keys)
	timer = Stopwatch.StartNew()
	for k in keys:
		v = db[k]
	timer.Stop()
	print 'Retrieved %d records in %s' % (len(keys), timer.Elapsed)	
	db.close()

def insertTest(keys):
	db = esedb.open(database, 'n', True)
	data = 'XXXXXXXXXXXXXXXX'
	timer = Stopwatch.StartNew()
	for x in keys:
		db[x] = data
	timer.Stop()
	db.close()
	return timer.Elapsed

def repeatedRetrieveTest(numretrieves):
	db = esedb.open(database, 'r')
	(key, data) = db.first()
	timer = Stopwatch.StartNew()
	for i in xrange(0, numretrieves):
		data = db[key]
	timer.Stop()
	db.close()
	return timer.Elapsed
	
def retrieveTest(keys):
	db = esedb.open(database, 'r')
	timer = Stopwatch.StartNew()
	for x in keys:
		data = db[x]
	timer.Stop()
	db.close()
	return timer.Elapsed

def scanTest():
	db = esedb.open(database, 'r')
	timer = Stopwatch.StartNew()
	i = 0
	for (k,v) in db:
		i += 1
	timer.Stop()
	db.close()
	return timer.Elapsed
	
# Basic test first
insertRetrieveTest()

# First insert the records in ascending order, this will be fastest
keys = range(1000000)
time = insertTest(keys)
print 'appended %d records in %s (lazy commit)' % (len(keys), time)

# Repeatedly retrieve the same record
numretrieves = 1000000
time = repeatedRetrieveTest(numretrieves)
print 'retrieved 1 record %d times in %s' % (numretrieves, time)

# Now scan all the records in key order. As the database was closed and reopened
# we will be starting with no data cached
time = scanTest()
print 'scanned %d records in %s' % (len(keys), time)

# Now retrieve all the records. As the database was closed and reopened
# we will be starting with no data cached
random.shuffle(keys)
time = retrieveTest(keys)
print 'randomly retrieved %d records in %s' % (len(keys), time)

# Now insert in random order (more likely)
random.shuffle(keys)
time = insertTest(keys)
print 'randomly inserted %d records in %s (lazy commit)' % (len(keys), time)

# Now scan all the records in key order. As the database was closed and reopened
# we will be starting with no data cached
time = scanTest()
print 'scanned %d records in %s' % (len(keys), time)

# Now retrieve all the records. As the database was closed and reopened
# we will be starting with no data cached
random.shuffle(keys)
time = retrieveTest(keys)
print 'randomly retrieved %d records in %s' % (len(keys), time)

