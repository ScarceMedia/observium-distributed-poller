

import atexit, logging, os, subprocess, json, time, sys

def warn(s):
	logging.error(s)
	print s

def error(s):
	warn("Quitting: " + s)
	sys.exit()

def info(s):
	logging.info(s)
	print s


#
# Get configuration from observium, start logging
#
observium_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
cmdargs = ['/usr/bin/env', 'php', '%s/config_to_json.php' % observium_root]
proc = subprocess.Popen(cmdargs, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
config = json.loads(proc.communicate()[0])

logging.basicConfig(filename=config['distributed-poller']['poll-producer']['log-file'],
	level=logging.DEBUG,
	format='%(asctime)s %(message)s',
	datefmt='%m/%d/%Y %H:%M:%S %p')


#
# Import 3rd party modules
#
try:
	import stompy
except ImportError:
	error("stompy module not found. Did you install it?")
try:
	import MySQLdb as mdb
except ImportError:
	error("MySQLdb module not found. Did you install it?")

#
# Connect to the database and to the message broker
#
db_connection = None
try:
	db_connection = mysqldb.connect(config['db_host'],
		config['db_user'],
		config['db_pass'],
		config['db_name'])
except mdb.OperationalError as e:
	error("MySQL connection error: " + e[1])

broker_connection = None
try:
	broker_connection = stompy.stomp.Stomp(
		hostname=config['distributed-poller']['messagebroker']['host'],
		port=config['distributed-poller']['messagebroker']['port'])
	broker_connection.connect(username=config['distributed-poller']['messagebroker']['username'],
	 	password=config['distributed-poller']['messagebroker']['password'])
	broker_connection.subscribe({'destination': config['distributed-poller']['poll-producer']['queue']})
except stompy.stomp.ConnectionError as e:
	error("Error connecting msg broker: " + e[1])
except stompy.frame.BrokerErrorResponse as e:
	error("Error connecting msg broker: " + str(e))




#
# Loop through devices that need to be polled, putting them in the queue
#
check_query = "SELECT device_id, last_polled FROM devices \
	WHERE TIMESTAMPDIFF(SECOND, devices.last_polled, NOW()) > %s"

try:
	cursor = db_connection.cursor()
	ttl = config['distributed-poller']['poll-producer']['ttl']
	cursor.execute(check_query % ttl)
	info("Found %i devices to update" % cursor.rowcount)

	row = cursor.fetchone()
	while row != None:
		try:
			device_id = utils.find_field(cursor, row, "device_id")
			last_polled = str(utils.find_field(cursor, row, "check_field"))
			toQueue = '{"device_id": %s, "last_polled": "%s"}' % (device_id, last_polled)
			broker_connection.send({'destination': \
				config['distributed-poller']['poll-producer']['queue'], 'body': toQueue})
		except stompy.frame.BrokerErrorResponse, e:
			warn("Error queueing %s: %s" % (str(device_id), str(e)), level=LogLevel.ERROR)
		except socket.error:
			error("Messagebroker connection has broken.")
		row = cursor.fetchone()
	time.sleep(check_interval * 60)
except mysqldb.OperationalError as e:
	error("MySQL error: ", e[1])
except mysqldb.Error as e:
	error("MySQL Error: ", e[1], level=LogLevel.ERROR)








