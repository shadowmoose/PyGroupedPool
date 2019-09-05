from pool import PyPool
import time
import unittest


class TestDriverInstalls(unittest.TestCase):

	def test_pool(self):
		""" Concurrency should properly work """
		start = time.time()
		# Create an example Pool, using a callback to print returned data & no error handling.
		pool = PyPool(tags={
			'test': 1,
			'ignored_pool': 5
		}, callback=lambda r: print('Returned:', r, ', Pending:', pool.pending))

		print(pool)  # Display the current Pool config.

		pool.adjust(None, 10)  # Adjust the "generic" pool size to fit all the upcoming threads, to run them concurrently.
		pool.adjust(None, 2)
		pool.adjust(None, 12)

		pool.ingest([5, 5, 5, 5, 5], 'test', time.sleep, [])

		print('Async Ingesting started... Awaiting.', pool)  # This will display before ingesting is done.

		pool.join()  # Wait for all ingesting & subprocess running to be complete.
		# Should take ~5 seconds, since they should all run at once.

		self.assertLess(time.time() - start, 15, 'Took longer than expected to run test - concurrency may be broken')


if __name__ == "__main__":
	unittest.main()
