txpool
======

![Build Status](https://travis-ci.org/escattone/txpool.svg?branch=master)

Summary
-------
A persistent process pool in Python for use with [Twisted](http://twistedmatrix.com).
Provides the ability to run Python callables asynchronously within a pool of
persistent processes, as long as the callables, their arguments, and their return
values are all picklable.

Installing
----------
```sh
pip install txpool
```
or
```sh
python setup.py install
```

Examples
--------
Here are some simple examples to give you the idea:

```python
    import glob
    from twisted.internet import reactor
    from twisted.internet.defer import inlineCallbacks
    import txpool

    pool = txpool.Pool()

    @inlineCallbacks
    def main():
        result = yield pool.apply_async(glob.glob, ('*.pdf',))
        print result
        reactor.stop()

    reactor.callWhenRunning(main)
    reactor.run()
```

The callable can instead be specified as a string, using dotted notation to
specify the full path to the callable.

```python
    from twisted.internet import reactor
    from twisted.internet.defer import inlineCallbacks
    import txpool

    pool = txpool.Pool()

    @inlineCallbacks
    def main():
        # You can provide an optional timeout (in seconds) for the call
        # (the default is None).
        try:
            result = yield pool.apply_async('glob.glob', ('*.pdf',), timeout=5)
        except txpool.PoolTimeout as e:
            result = e
        print result
        reactor.stop()

    reactor.callWhenRunning(main)
    reactor.run()
```

The *txpool.Pool* class can be explicitly sized, asked to log its actions,
and/or given a custom name.

```python
    import logging
    from twisted.internet import reactor
    from twisted.internet.defer import inlineCallbacks, gatherResults
    import txpool

    logger = logging.getLogger('example')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    pool = txpool.Pool(size=5, log=logger, name='twisting-by-the-pool')

    @inlineCallbacks
    def main():
        calls = ('math.factorial',) * 5
        args = [(n,) for n in range(150780, 150785)]

        # You can wait until the pool is at full-strength (providing an
        # optional timeout if desired), but it's not required before
        # calling the "apply_async" method.  Jobs are queued until a
        # worker process is available.
        try:
            yield pool.on_ready(timeout=10)
        except txpool.PoolTimeout as e:
            results = e
        else:
            results = yield gatherResults(map(pool.apply_async, calls, args))

        print results

        try:
            # You can gracefully close the pool, which ensures all jobs
            # already queued are completed before shutting down...
            yield pool.close(timeout=10)
        except txpool.PoolTimeout as e:
            print e
            # ...or you can use force and immediately send SIGKILL to each
            # process in the pool.
            yield pool.terminate(timeout=10)

        reactor.stop()

    reactor.callWhenRunning(main)
    reactor.run()
```
