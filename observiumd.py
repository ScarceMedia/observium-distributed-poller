'''
File: observium/crawler/observiumd.py
Author: John Chilton
Created: 9/16/2013

Purpose: Part of the Observium device polling service, this class
		is a base class for other daemons
'''

import atexit, logging, os, subprocess, json, time, threading
import stompy, MySQLdb as mysqldb

class StompError:
	pass

class DBError:
	pass

class LogLevel(object):
	WARN = 1
	INFO = 2
	ERROR = 3
	CRITICAL = 4

class ObserviumDaemon(object):
	db_connection = None
	db_connected = False

	broker_connection = None
	broker_connected = False

	config = {}

	reconnect_interval = 1

	worker = None

	observium_root = os.path.dirname(
							os.path.dirname(os.path.realpath(__file__)))
	config_prefix = None

	def _log(self, msg, level=LogLevel.INFO):
		prefix = ""
		if threading.current_thread().name != "MainThread":
			prefix = threading.current_thread().name + ": "
		if level == LogLevel.INFO:
			logging.info(prefix + msg)
		elif level == LogLevel.WARN:
			logging.warn(prefix + msg)
		elif level == LogLevel.ERROR:
			logging.error(prefix + msg)
		elif level == LogLevel.CRITICAL:
			logging.critical(prefix + msg)

	def __init__(self, config_prefix):
		self.config_prefix = config_prefix
		self._get_config()

		logging.basicConfig(filename=self.config['crawler'][self.config_prefix]['log_file'],
			level=logging.DEBUG,
			format='%(asctime)s %(message)s',
			datefmt='%m/%d/%Y %H:%M:%S %p')

		self._log("Startup")

		self._connect_db()
		self._connect_message_broker()

		atexit.register(self.shutdown)


	def shutdown(self):
		self._log("Shutdown")
		if self.db_connected:
			self.db_connection.close()
		if self.broker_connected:
			try:
				self.broker_connection.disconnect()
			except:
				pass


	def _get_config(self):
		# this file is in <observium_root>/polld. get <observium_root>.
		# this function adapted from poller-wrapper.py by Job Snijders
		cmdargs = ['/usr/bin/env', 'php', '%s/config_to_json.php' % self.observium_root]
		proc = subprocess.Popen(cmdargs, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
		self.config = json.loads(proc.communicate()[0])


	'''
	Returns True if a connection was (re-)established,
	Returns False if a connection could not be (re-)established,
	'''
	def _connect_db(self):
		self._get_config()
		if self.db_connection != None:
			self.db_connection.close()
		try:
			self.db_connection = mysqldb.connect(self.config['db_host'],
						self.config['db_user'],
						self.config['db_pass'],
						self.config['db_name'])
			self._log("Connected to database")
			self.db_connected = True
			return True
		except mysqldb.OperationalError:
			self._log("Connecting to the database failed.", level=LogLevel.ERROR)
			return False


	def _connect_message_broker(self):
		self._get_config()
		try:
			self.broker_connection = stompy.stomp.Stomp(
									hostname=self.config['crawler']['messagebroker']['host'],
									port=self.config['crawler']['messagebroker']['port'])
			self.broker_connection.connect(username=self.config['crawler']['messagebroker']['username'],
										 password=self.config['crawler']['messagebroker']['password'])
			self.broker_connection.subscribe({'destination': self.config['crawler'][self.config_prefix]['queue']})
			self._log("Connected to message broker")
			self.broker_connected = True
			return True
		except stompy.stomp.ConnectionError as e:
			self._log("Error connecting msg broker: " + e[1], level=LogLevel.ERROR)
			return False
		except stompy.frame.BrokerErrorResponse as e:
			self._log("Error connecting msg broker: " + str(e), level=LogLevel.ERROR)


	def run(self):
		while True:
			if not self.db_connected:
				if not self._connect_db():
					time.sleep(self.reconnect_interval * 60)
			if not self.broker_connected:
				if not self._connect_message_broker():
					time.sleep(self.reconnect_interval * 60)
			if self.broker_connected and self.db_connected:
				try:
					self.worker()
				except StompError:
					self.broker_connected = False
				except DBError:
					self.db_connected = False

