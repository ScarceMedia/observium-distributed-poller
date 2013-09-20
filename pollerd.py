'''
File: observium/crawler/consumer.py
Author: John Chilton
Created: 9/15/2013

Purpose: Part of the Observium crawling service, this class consumes
		messages from a queue and looks up devices using the observium
		poller script
'''



# standard libraries	
import logging, socket, sys, time, threading, subprocess, json

# packages installed via pip or via package manager
import MySQLdb as mysqldb, stompy

# from this package
import utils, observiumd
from observiumd import LogLevel


class PollDaemon(observiumd.ObserviumDaemon):

	threads = []

	def _spawn_consumer(self):
		d = PollThread()
		d.run()

	'''
	process_script_name should be something like discover.php or poller.php
	check_field should be something like last_discovered or last_polled
	'''
	def __init__(self):
		super(PollDaemon, self).__init__("pollerd")
	
	def run(self):
		for i in range(self.config['crawler']['pollerd']['num_threads']):
			self.threads.append(threading.Thread(target=self._spawn_consumer))
			self.threads[i].start()




class PollThread(observiumd.ObserviumDaemon):
	def __init__(self):
		super(PollThread, self).__init__('pollerd')

	def shutdown(self):
		self._log("Poller Shutdown")
		if self.db_connected:
			self.db_connection.close()
		if self.broker_connected:
			try:
				self.broker_connection.disconnect()
			except:
				pass

	def _do_work(self):
		frame = None
		poll_obj = None
		last_polled_real = None
		try:
			frame = self.broker_connection.receive_frame(nonblocking=True)
		except stompy.frame.UnknownBrokerResponseError as e:
			if str(e) == "empty reply":
				self._log("Empty response from messagebroker. Queue may be empty.")
				time.sleep(60)
			else:
				self._log("UnknownBrokerResponseError caught while receiving a frame")
			return
		try:
			touch_obj = json.loads(frame.body)
		except ValueError:
			self._log("Couldn't parse JSON: " % frame.body, level=LogLevel.ERROR)
			return
		try:
			cursor = self.db_connection.cursor()
			cursor.execute("SELECT last_polled FROM devices WHERE device_id='%s'" % int(touch_obj['device_id']))
			last_polled_real = cursor.fetchone()[0]
		except mysqldb.Error as e:
			self._log("MySQL error: " + str(e), level=LogLevel.ERROR)
			last_polled_queue = utils.datetime_from_str(poll_obj['last_touched'])

			if last_polled_real > last_polled_queue:
				self._log("Last polled time for device %s is more recent than in queue: " % touch_obj['device_id'])
		# these few lines adapted from poller-wrapper.py by Job Snijders
		processor_path = self.observium_root + "/poller.php"
		start_time = time.time()
		command = "/usr/bin/env php %s -h %s >> /dev/null 2>&1" % (processor_path, touch_obj['device_id'])
		self._log(command)

		try:
			subprocess.check_call(command, shell=True)
		except subprocess.CalledProcessError as e:
			self._log("Polling did not complete successfully.", level=LogLevel.ERROR)


	worker = _do_work


if __name__ == "__main__":
	mydaemon = PollDaemon()
	mydaemon.run()

