from pool import PyPool
import time
import unittest
import threading
import os


class TestPool(unittest.TestCase):

	def setUp(self):
		# Create an example Pool, using a callback to print returned data & no error handling.
		self.pool = PyPool(iteration=True, tags={
			'test': 1,
			'ignore': 1
		}, callback=lambda r: print('Returned:', r, ', Pending:', self.pool.pending))
		self.count = 0

	def test_async(self):
		""" Concurrency should properly work """
		start = time.time()

		self.pool.adjust('test2', 10)
		self.pool.ingest([2, 2, 2, 2, 2, 2, 2, 2, 2, 2], 'test2', time.sleep, [])

		self.pool.join()  # Wait for all ingesting & subprocess running to be complete.
		# Should take ~5 seconds, since they should all run at once.

		self.assertLess(time.time() - start, 15, 'Took longer than expected to run test - concurrency may be broken')

	def test_adjust(self):
		""" Adjust should properly add/remove generic slots """
		pool = self.pool
		pool.adjust('test', 10)
		pool.adjust(None, 0)

		pool.adjust('test', 5, use_general_slots=True)
		self.assertEqual(pool.get_tags()['test'], 5, 'Incorrect slots set for test!')
		self.assertEqual(pool.get_tags()[None], 5, 'Incorrect slots adjusted for general!')

		pool.adjust('test', 1, use_general_slots=True)
		self.assertEqual(pool.get_tags()['test'], 1, 'Incorrect slots set for test!')
		self.assertEqual(pool.get_tags()[None], 9, 'Incorrect slots adjusted for general!')

		self.assertEqual(pool.get_tags()['ignore'], 1, 'Ignored pool should not have been touched!')

		self.assertEqual(pool._total, 11, msg='Incorrect total count was left after adjustments.')

		print(pool)
		
		with self.assertRaises(Exception, msg='Failed to raise Error on invalid pool size increase!'):
			pool.adjust('test', 11, use_general_slots=True)

	def test_stop(self):
		""" Stop should correctly exit """
		start = time.time()

		self.pool.adjust('test', 1)
		self.pool.adjust(None, 0)

		self.pool.ingest([60, 60, 60], 'test', time.sleep, [])

		self.pool.stop()
		print('Stop completed.')

		self.assertLess(time.time() - start, 15, 'Took longer than expected to run test - concurrency may be broken')

	def test_iter(self):
		""" The iterator should work """
		self.pool.ingest([1, 2, 3, 4, 5, 6, 7, 8], 'test', fnc)
		self.pool.callback(None)
		count = 0
		for r in self.pool:
			count += r
		self.assertEqual(count, 36, 'Did not get all results back from iterator!')

	def test_callback(self):
		""" The callback method should work """
		def cb(val):
			self.count += val

		def err(e):
			self.count += 100

		self.pool.callback(cb=cb)
		self.pool.on_error(err)

		for i in range(4):
			self.pool.put('test', fnc, [i])
		self.pool.put('test', fnc, [])  # Trigger an error, which will be caught and increment counter by 100.

		self.pool.join()

		self.assertEqual(self.count, 106, 'Callback/Error was not triggered enough times!')

	def test_single_callback(self):
		""" Individual tasks should support custom callbacks """
		def cb(val):
			print('cb called')
			self.count += val

		def err(e):
			print('err called', e)
			self.count += 100

		self.pool.callback(cb=None)  # Clear any built-in handlers for custom callback test.
		self.pool.on_error(None)

		for i in range(4):
			self.pool.put('test', fnc, [i], callback=cb)
		self.pool.put('test', fnc, [], error=err)  # Trigger an error, which will be caught and increment counter by 100

		self.pool.join()

		self.assertEqual(self.count, 106, 'Custom callback/error was not triggered enough times!')


def fnc(num):
	return num


def timeout():
	time.sleep(60)
	print('Timed out!')
	os._exit(103)


timeout_thread = threading.Thread(target=timeout, daemon=True)
timeout_thread.start()


if __name__ == "__main__":
	unittest.main()
