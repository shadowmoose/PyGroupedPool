# Grouped Pool [![badge](https://action-badges.now.sh/shadowmoose/PyGroupedPool)](https://github.com/shadowmoose/PyGroupedPool/actions)[![codecov](https://codecov.io/gh/shadowmoose/PyGroupedPool/branch/master/graph/badge.svg)](https://codecov.io/gh/shadowmoose/PyGroupedPool)

*This is currently a proof-of-concept. No release currently exists.*

This project is a wrapper around the Python "Pool" implementation for multiprocessing.

It extends the basic functionality to add a few notable changes:

+ The Pool object supports group-based tags, each of which can guarantee their own amount of dedicated process 'slots'.
+ A generic tag ('None') can also be provided, to allow all tagged groups to burst above their base limits as-needed.
+ Group sizes can be adjusted live while running, to allow your program to reallocate subprocesses priority in realtime.
+ The logic to launch infinite sub-processes has been streamlined to prevent excessive memory usage.
+ The Pool supports data & error callbacks, or a simple generator to iterate the results as they're returned.

## Example:

This demo creates a Pool object with three groups. Each group can be named whatever you want. 
In this example, we emulate a potential webserver + background-worker setup.

+ All processes submitted to group `webserver` are guaranteed at least one dedicated slot.
+ The `processors` group, in this example, is guaranteed 5 slots.
+ The `None` group is the "general purpose" group, and all tagged groups may use these if they are out of dedicated slots.
```python
# Create an example Pool, using callbacks to print returned data & errors.
pool = PyPool(tags={
    'webserver': 1,
    'processors': 5,
    None: 10
}, callback=lambda r: print('Returned:', r, ', Pending:', pool.pending), on_error=print)
```

Once the Pool has been created, it's easy to submit new tasks. The simplest way is to use the (asynchronous) `ingest` method.

This method accepts any iterable collection of values, and submits them all to the given group.
To avoid excessive memory consumption, it will wait to create new subprocesses until there is an open slot for them.

```python
pool.ingest([1, 2, 4, 5, 'etc...'], 'tag_group', function_to_run, ['extra_func_arguments'])
```

If you don't need to bulk-add, or you need more control over the functionality, you can also add sub-processes individually in a block fashion:

```python
# You can also pass custom result/error callbacks to override the defaults:
result = pool.put('tag_name', function_name, ('arg1', 'arg2', 'etc...'), callback=print, error=print)  
# A result object is always returned, just in case you want to handle it manually.
```


## Resizing Pool Group Capacity
If needed, each group can be resized at-will, to allow for dynamic shifting of priorities.
Resizing will play nicely with running threads, only removing and adding capacity as it becomes available.

By default, resizing creates or destroys new subprocess slots. 
If you specify the 'use_general_slots' flag, the change in slots will instead be added or removed from the general purpose ('None tag') group.
```python
pool = PyPool(tags={
    'test': 1,
    'ignored_pool': 5
}, on_error=print)


pool.adjust('test', 10)  # The group 'test' now supports up to 10 concurrent processes. 9 slots have been created.

# You may also move slots to/from the general pool.
# After this call, 'ignored_pool' group will only have 1 process slot available -
#   but the generic group (None tag) will gain the removed 4 process slots:
pool.adjust('ignored_pool', 1, True)

pool.adjust('test', 14, True)  # The 'test' group now has 14 reserved slots for processes, and the general pool has 0.
```

## Iterator Example:
If you'd prefer to use an iterator instead of an async callback, simply create a Pool without a data callback:
```python
pool = PyPool(tags={
    'test': 1,
    'ignored_pool': 5
}, on_error=print)

pool.adjust(None, 10)  # Adjust the "generic" pool size to fit all the upcoming threads, to run them concurrently.

pool.ingest([5, 5, 5, 5, 5], 'test', time.sleep, [])  # Asynchronously run a subprocess for each value, using util function.

for v in pool:
    print(v)  # Prints all data as it becomes available. Add extra logic to exit, or this will wait for any new data.
```

## More documentation:
There's more functionality, such as `join` or `stop`, and the easiest way to learn about them is to [read the docs](./pool.py) for each method.
