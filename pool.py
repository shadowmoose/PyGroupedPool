import multiprocessing
from threading import Thread
from queue import Empty
import traceback
import threading


class PyPool:
	def __init__(self, limit=0, tags=None, callback=None, error_handler=None, iteration=False):
		"""
		Create a new Pool, and start the managing threads for it.

		:param limit: If set, this is the maximum allowed subprocess "slots" to use for initial setup.
		:param tags: If provided, this should be an Object. Each key should be a tag name, and each value its slot count.
		:param callback: This function will be called with the returned data from each sub-process. Disables iteration.
		:param error_handler: This function will be called with any Exceptions bubbled up from any sub-process.
		:param iteration: If True, track all returned values and pass them out via the built-in __iter__ method.
		"""
		self._tags = {}
		self._results = multiprocessing.Queue()
		self._iteration = iteration
		self._cb = callback
		self._tag_lock = multiprocessing.RLock()
		self._pending_lock = threading.RLock()
		self._ingest_lock = threading.RLock()
		self._stop = threading.Event()
		self._total = 0
		self._pool = None
		self._pending = []
		self._thread = Thread(target=self._monitor, daemon=True)
		self._error_handler = error_handler if error_handler else lambda err: traceback.print_exc()
		self._ingest_streams = []

		tot = 0
		if tags:
			for k, v in tags.items():
				if not k:
					k = None
				self._set(k, v)
				tot += v
			if limit and limit - tot:
				self._set(None, limit - tot)
		else:
			self._set(None, limit)
		self._recount_total()
		self._thread.start()

	def _set(self, tag, limit):
		self._tags[tag] = {
			'limit': limit,
			'sem': multiprocessing.Semaphore(limit)
		}

	def _recount_total(self):
		with self._tag_lock:
			self._total = sum(self._tags[k]['limit'] for k in self._tags.keys())
			assert self._total > 0, 'Invalid quantity of threads provided!'
			self._pool = multiprocessing.Pool(self._total)

	def _sem(self, tag):
		if tag not in self._tags or not self._tags[tag]['limit']:
			return None
		return self._tags[tag]['sem']

	def _monitor(self):

		def check(r):
			# noinspection PyBroadException
			try:
				r['res'].successful()  # Throws if not completed, otherwise ignore and indicate it has resolved.
				return True
			except Exception:
				return False
		while not self._stop.is_set():
			self._stop.wait(.01)
			with self._pending_lock:
				finished = filter(check, self._pending)
				for f in finished:
					try:
						val = f['res'].get()
						self._finish(val, f['tag'], f['callback'])
					except Exception as e:
						if f['error']:
							f['error'](e)
						elif self._error_handler:
							self._error_handler(e)
						else:
							raise Exception('Unhandled exception from process call! Use on_error() to set a handler!')
					finally:
						self._pending.remove(f)

	def _finish(self, res, tag, callback):
		self._sem(tag).release()  # Release one of the Semaphore for this tag.
		if self._stop.is_set():
			return
		if callback:
			callback(res)
		elif self._cb:
			self._cb(res)
		elif self._iteration:
			self._results.put(res)

	def _run(self, tag, fnc, args, callback=None, error=None):
		res = self._pool.apply_async(fnc, args=args)  # This method supports a callback, but it doesn't run on error.
		with self._pending_lock:
			self._pending.append({'tag': tag, 'res': res, 'callback': callback, 'error': error})
		return res

	def iter(self):
		"""
		Creates a generator for this Pool, which returns results from the running sub-processes in no specific order.
		The generator exits once all running sub-processes return, if no Ingest threads are currently running.

		:return: Iterable Generator of results.
		"""
		if not self._iteration:
			raise Exception('Iteration is disabled on this Pool!')
		brk = False
		while not self._stop.is_set():
			if self._cb:
				raise Exception('Error: Cannot iterate while callback is activated on pool!')
			try:
				yield self._results.get(block=True, timeout=.1)
				brk = False
			except Empty:
				if self._stop.is_set():
					break
				with self._pending_lock:
					if not len(self._pending):
						with self._ingest_lock:
							if not len(self._ingest_streams):
								if brk:
									break
								brk = True

	def put(self, tag, fnc, args=tuple(), callback=None, error=None):
		"""
		Launch the given function in a new Process. Requires a tag, which it uses to determine concurrency limits.
		
		This method blocks until the sub-process has been started.

		:param tag: The "group" this subprocess should run as. Used for limiting concurrent count based off tags.
		:param fnc: The function to run.
		:param args: The arguments to provide to this function. Tuple.
		:param callback: If provided, use this function as the callback for this result.
		:param error: If provided, use this function as the callback to handle errors from the subprocess.
		:return: a `multiprocessing.pool.AsyncResult` instance, in case you have use for it.
		"""
		sems = [self._sem(tag), self._sem(None)]
		args = tuple(args)
		while any(sems) and not self._stop.is_set():
			for sem in sems:
				if not sem:
					continue
				if sem.acquire(block=True, timeout=0.05):
					return self._run(tag, fnc, args, callback, error)
		if not self._stop.is_set():
			raise KeyError('No valid pool could be found for the given tag: %s' % tag)

	@property
	def pending(self):
		"""
		Get an (approximate) count of the total sub-processes that are currently running.

		:return: An estimated count of the running sub-processes.
		"""
		with self._pending_lock:
			return len(self._pending)

	def adjust(self, tag, new_limit, use_general_slots=False):
		"""
		Change the total limit of the given tag.
		This method will block until enough slots can be freed/removed to match the new value.

		:param tag: The tag to change.
		:param new_limit: The new limit for this tag.
		:param use_general_slots: If True, each freed/allocated thread slot will be moved to/from the general pool.
		"""
		new_limit = max(0, new_limit)
		with self._tag_lock:
			dat = self._tags[tag] if tag in self._tags else None
			if use_general_slots and None not in self._tags:
				self._set(None, 0)
			if not dat:
				self._set(tag, new_limit)
			else:
				while dat['limit'] < new_limit:
					dat['sem'].release()
					dat['limit'] += 1
					if use_general_slots:
						if self._tags[None]['limit'] > 0:
							self._tags[None]['sem'].acquire()
							self._tags[None]['limit'] -= 1
						else:
							raise IndexError('Unable to move the correct amount of slots from the General Pool.')
				while dat['limit'] > new_limit:
					dat['sem'].acquire()
					dat['limit'] -= 1
					if use_general_slots:
						self._tags[None]['sem'].release()
						self._tags[None]['limit'] += 1
			self._recount_total()

	def callback(self, cb):
		"""
		Sets a callback function to handle returned Data.
		If set, this disables the iterable Queue, and instead passes the returned result of every Subprocess to this function.

		:param cb: A function, which accepts one parameter - the returned data. EG: `lambda data: print('Result:', data)`.
		"""
		self._cb = cb

	def on_error(self, error_handler):
		"""
		Similar to `callback()`, this registers a function to handle any Exceptions that are encountered in any subprocess.
		This will override the default handler, which simply prints & ignores all Errors.

		:param error_handler: A function, which accepts one parameter - the Error. EG: `lambda data: print('Error:', data)`.
		"""
		self._error_handler = error_handler

	def stop(self):
		"""
		Cleanly shut down this Pool. While everything here should shut down on its own when the main thread exits,
		this method can be used instead to kill the Pool at-will.
		
		Calling this is destructive, and will immediately interrupt any active ingestor threads -
		as well as any result handling.
		Use the `join()` method before calling this if you want to wait for all processing to properly finish first.
		"""
		self._stop.set()

	def ingest(self, iterable, tag, fnc, args=(), callback=None, error=None):
		"""
		Non-blocking convenience method - this Iterates through the given Iterable, 
		and calls `self.put()` with each element.
		Each element in the iterable will be passed as the first parameter to the given function.

		This will launch a new Daemon thread, which will run in the background.
		You may start multiple Ingestor Threads to run concurrently, and await them all using the Pool's `join()` method.

		:param iterable: The input, must be any iterable.
		:param tag: The "group" this subprocess should run as. Used for limiting concurrent count based off tags.
		:param fnc: The function to run.
		:param args: The arguments to provide to this function. Each item in the iterable will be prepended to these.
		:param callback: If provided, use this function as the callback for each result.
		:param error: If provided, use this function as the callback to handle each error from the subprocesses.
		:return: The Thread, already started, which handles adding the given values.
		"""
		_t = None

		def ing():
			for e in iterable:
				if self._stop.is_set():
					break
				self.put(tag, fnc, (e, *args), callback, error)
			with self._ingest_lock:
				self._ingest_streams.remove(_t)
		_t = Thread(daemon=True, target=ing)
		with self._ingest_lock:
			self._ingest_streams.append(_t)
		_t.start()
		return _t

	def join(self):
		"""
		Awaits all running Ingestor threads, as well as any pending processes.
		:return:
		"""
		for th in self._ingest_streams:
			th.join()
		while self.pending:
			self._stop.wait(0.1)

	def get_tags(self):
		"""
		Get a map of `tag: limit` for all tags tracked by this Pool.

		:return: Map of tags and their limits.
		"""
		return {k: v['limit'] for k, v in self._tags.items()}

	def __iter__(self):
		return self.iter()

	def __repr__(self):
		return '''<TagPool: %s || Total: %s>''' % (self._tags, self._total)
