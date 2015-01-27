#
# The MIT License (MIT)
#
# Copyright (c) 2015 Ryan Johnson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

__all__ = ['Pool', 'PoolError', 'PoolTimeout', 'cpu_count']


import os
import sys
import cPickle as pickle
from collections import deque
from logging import INFO, ERROR
from multiprocessing import cpu_count
from twisted.internet.task import deferLater
from twisted.protocols.basic import NetstringReceiver
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import succeed, fail, Deferred


DEFAULT_POOL_SIZE = 2


class PoolError(Exception):
    pass


class PoolTimeout(Exception):
    pass


class Pool(object):
    """
    Creates a pool of worker processes which persist to asynchronously
    execute jobs, where jobs are simply callables and their associated
    positional and keyword arguments.

    size:  target number of worker processes in the pool, or if None,
           the result of cpu_count() (see link below) will be used, or
           if that fails, the default size of two will be used

    init_call,
    init_args:  an initial callable and its arguments that will be
                called once as "init_call(*init_args)" within every
                process after it has started

    log:  a python standard logger object or None

    name:  the name of the pool, or if None, the result of "id(self)"
           will be used

    (http://docs.python.org/2.7/library/multiprocessing.html#miscellaneous)
    """
    def __init__(self, size=None, init_call=None, init_args=None, log=None,
                 name=None):
        self._manager = PoolManager(self, size=size, init_call=init_call,
                                    init_args=init_args, log=log, name=name)

    @property
    def size(self):
        """
        The target size of the process pool.
        """
        return self._manager.size

    @property
    def name(self):
        """
        The name of the process pool.
        """
        return self._manager.name

    def get_number_of_workers(self):
        """
        Returns the current number of running worker processes in the pool.
        """
        return len(self._manager.workers)

    def on_ready(self, timeout=None):
        """
        Returns an instance of twisted.internet.defer.Deferred whose callback
        is called with this pool instance when the pool of worker processes is
        up and running at the requested size.  An optional timeout period (in
        seconds, float or int) can be specified after which, if the pool is not
        yet ready, the returned deferred's errback will be called with an
        instance of PoolTimeout.
        """
        return self._manager.on_ready(timeout)

    def on_closure(self, timeout=None):
        """
        Returns an instance of twisted.internet.defer.Deferred whose callback
        is called with this pool instance when the pool has been closed and
        all of the worker processes in the pool have ended.  An optional
        timeout period (in seconds, float or int) can be specified after which,
        if the pool is not yet closed, the returned deferred's errback will be
        called with an instance of PoolTimeout.
        """
        return self._manager.on_closure(timeout)

    def apply_async(self, call, args=None, kwargs=None, timeout=None):
        """
        Asynchronously executes "call(*args, **kwargs)" within one of the
        pool's worker processes.

        call:  a string that provides a fully-qualified callable name, e.g.
               'os.path.exists', or a picklable callable.

        args, kwargs:  optional positional and/or keyword arguments for "call"
        
        timeout: optional timeout which, if provided, specifies how long this
                 call can run before its deferred's errback is fired with a
                 PoolTimeout exception

        Returns an instance of twisted.internet.defer.Deferred whose callback
        is fired with the result, or whose errback is fired with an instance
        of PoolError (in the case of an error) or PoolTimeout (in the case of
        a timeout).
        """

        if self._manager.closing:
            return fail(PoolError('Pool "%s": apply_async() called '
                                  'after closing.' % self.name))
        else:
            timeout_msg = ('Pool %(name)s: apply_async() call not '
                           'completed within %(timeout)s seconds.')
            job = Job(call, args, kwargs, timeout, timeout_msg, name=self.name)
            self._manager.queue_job(job)
            return job.deferred

    def close(self, timeout=None):
        """
        Stops all of the workers in the pool, but only after all of the
        currently-queued jobs have been completed.

        Returns an instance of twisted.internet.defer.Deferred whose callback
        will be fired with this Pool object when all of the queued jobs have
        been completed and all of the worker processes have ended, or whose
        errback will be called with an instance of PoolTimeout if the pool
        has not yet closed within the timeout period given (seconds).
        """
        return self._manager.close(timeout)

    def terminate(self, timeout=None):
        """
        Immediately sends a SIGKILL to all of the worker processes in
        the pool.

        Returns an instance of twisted.internet.defer.Deferred whose callback
        will be fired with this Pool object when all of the worker processes
        have ended, or whose errback will be called with an instance of
        PoolTimeout if the pool has not yet closed within the timeout period
        given (seconds).
        """
        return self._manager.terminate(timeout)


class Job(object):
    """
    Represents a deferred "call(*args, **kwargs)".
    """

    __slots__ = ('call', 'args', 'kwargs', 'deferred', 'worker', '_log_lines')

    def __init__(self, call, args=None, kwargs=None, timeout=None,
                 timeout_msg=None, **timeout_msg_kwargs):
        self.call = call
        self.args = args
        self.kwargs = kwargs
        self.worker = None
        self._log_lines = None

        def canceller(_):
            if self.worker:
                self.worker.send_signal('TERM')

        self.deferred = make_deferred_with_timeout(canceller=canceller,
                                                   timeout=timeout,
                                                   timeout_msg=timeout_msg,
                                                   **timeout_msg_kwargs)

    def log(self, line):
        if self._log_lines is None:
            self._log_lines = []
        self._log_lines.append(line)

    def get_log(self):
        return '\n'.join(self._log_lines or ())

    def __repr__(self):
        if isinstance(self.call, basestring):
            name = self.call
        else:
            name = repr(self.call)

        def args():
            if self.args:
                for arg in self.args:
                    yield repr(arg)
            if self.kwargs:
                for item in self.kwargs.iteritems():
                    yield '%s=%r' % item

        return ('<%s object at %#x: %s(%s)>' %
                (self.__class__.__name__, id(self), name, ', '.join(args())))


class Initializer(Job):
    """
    A Job used to initialize a worker process.
    """

    __slots__ = ()


class PoolManager(object):
    """
    The private "guts" of the Pool.
    """
    def __init__(self, pool, size=None, init_call=None, init_args=None,
                 log=None, name=None):
        self.pool = pool
        self.workers = set()
        self.closing = False
        self.job_queue = deque()
        self.init_call = init_call
        self.init_args = init_args
        self.log = MaybeLogger(log)
        self.name = name or id(self)
        self.deferreds_on_ready = deque()
        self.deferreds_on_closure = deque()

        if size is None:
            try:
                size = cpu_count()
            except NotImplementedError:
                size = DEFAULT_POOL_SIZE

        if size < 1:
            raise ValueError("the pool size must be > 0")

        self.size = size

        for _ in xrange(size):
            self.start_worker()

    def is_ready(self):
        return len(self.workers) >= self.size

    def is_closed(self):
        return self.closing and not (self.workers or self.job_queue)

    def on_ready(self, timeout=None):
        if self.closing:
            return fail(PoolError('Pool "%s": on_ready() called '
                                  'after closing.' % self.name))

        if self.is_ready():
            return succeed(self.pool)
        
        msg = 'Pool %(name)s: not ready within %(timeout)s seconds.'
        d = make_deferred_with_timeout(timeout=timeout, timeout_msg=msg,
                                       name=self.name)
        self.deferreds_on_ready.append(d)
        return d

    def on_closure(self, timeout=None):
        if self.is_closed():
            return succeed(self.pool)

        msg = 'Pool %(name)s: not closed within %(timeout)s seconds.'
        d = make_deferred_with_timeout(timeout=timeout, timeout_msg=msg,
                                       name=self.name)
        self.deferreds_on_closure.append(d)
        return d

    def start_worker(self):
        worker = Worker(on_start=self.on_worker_started,
                        on_end=self.on_worker_ended,
                        on_result=self.on_worker_result,
                        on_log=self.on_worker_log)

        from twisted.internet import reactor
        # Setting "env=None" passes os.environ to the child process.
        reactor.spawnProcess(worker.protocol, sys.executable, [sys.executable,
                             '-m', 'txpool.worker'], env=None)

    def on_worker_started(self, worker):
        self.log.info('Pool "%s": process %d started.', self.name, worker.pid)

        self.workers.add(worker)

        if self.init_call:
            job = Initializer(self.init_call, self.init_args)

            def init_success(result):
                self.log.info('Pool "%s": process %d initialized.',
                               self.name, worker.pid)

            def init_failure(failure):
                self.log.info('Pool "%s": process %d failed to initialize.',
                               self.name, worker.pid)

            job.deferred.addCallbacks(init_success, init_failure)
        else:
            job = None

        self.employ(worker, job)

        if self.is_ready():
            while self.deferreds_on_ready:
                d = self.deferreds_on_ready.popleft()
                if not d.called:
                    d.callback(self.pool)

    def on_worker_ended(self, worker, job, exit_code, signal):
        msg = ('Pool "%s": process %d ended (exit-code=%r, signal=%r) while '
               '%s.' % (self.name, worker.pid, exit_code, signal,
                        (job and ('running %r' % job)) or 'idle'))

        self.log.log(ERROR if exit_code else INFO, msg)

        self.workers.remove(worker)

        replace_worker = not (isinstance(job, Initializer) or
                              (self.closing and not self.job_queue))

        if replace_worker:
            # Start a replacement process.
            self.start_worker()

        if job and not job.deferred.called:
            # Start the errback chain on the job's deferred.
            job.deferred.errback(PoolError(msg + '\n' + job.get_log()))

        if (not replace_worker) and self.is_closed():
            while self.deferreds_on_closure:
                d = self.deferreds_on_closure.popleft()
                if not d.called:
                    d.callback(self.pool)

    def on_worker_result(self, worker, job, result):
        self.log.debug('Pool "%s" [%d]: %r: %r' %
                       (self.name, worker.pid, job, result))

        self.employ(worker)

        # Start the callback chain on the job's deferred.
        if job and not job.deferred.called:
            job.deferred.callback(result)

    def on_worker_log(self, worker, job, line):
        self.log.debug('Pool "%s" [%d]: %s' % (self.name, worker.pid, line))
        if job:
            job.log(line)

    def employ(self, worker, job=None):
        while job is None and self.job_queue:
            job = self.job_queue.popleft()
            if job.deferred.called:
                # This job has been cancelled, so skip it.
                job = None

        if job:
            worker.work_on(job)
        elif self.closing:
            self.log.info('Pool "%s": closing process %d.', self.name,
                          worker.pid)
            worker.retire()

    def employ_idle_workers(self):
        for worker in self.workers:
            if worker.job is None:
                self.employ(worker)

    def queue_job(self, job):
        self.job_queue.append(job)
        self.employ_idle_workers()

    def close(self, timeout=None):
        self.closing = True
        self.employ_idle_workers()
        return self.on_closure(timeout)

    def terminate(self, timeout=None):
        self.closing = True
        self.job_queue.clear()
        for worker in self.workers:
            worker.send_signal('KILL')
        return self.on_closure(timeout)


class Worker(object):
    """
    A Worker represents a process that persists to work on jobs.
    """

    def __init__(self, on_start, on_end, on_result, on_log):
        self.pid = None
        self.job = None
        self.prev_call = None
        self.callback_on_log = on_log
        self.callback_on_end = on_end
        self.callback_on_start = on_start
        self.callback_on_result = on_result
        self.protocol = WorkerProtocol(self)

        def log_line(line):
            if self.callback_on_log:
                self.callback_on_log(self, self.job, line)

        self._log_handler = Liner(log_line)

    def work_on(self, job):
        self.push_job(job)
        if job.call != self.prev_call:
            self.send_pickled(job.call)
            self.prev_call = job.call
        self.send_pickled((job.args, job.kwargs))

    def on_start(self, pid):
        self.pid = pid
        if self.callback_on_start:
            self.callback_on_start(self)

    def on_end(self, reason):
        self._log_handler.finish()
        if self.callback_on_end:
            v = reason.value
            self.callback_on_end(self, self.pop_job(), v.exitCode, v.signal)

    def on_result(self, result):
        self._log_handler.finish()
        if self.callback_on_result:
            self.callback_on_result(self, self.pop_job(), result)

    def on_log(self, data):
        self._log_handler.add(data)

    def retire(self):
        # This will end the process.
        self.send_pickled(None)

    def send_signal(self, signal):
        transport = self.protocol.transport
        if transport:
            transport.signalProcess(signal)

    def send_pickled(self, data):
        transport = self.protocol.transport
        if transport:
            transport.write(pickle.dumps(data))

    def push_job(self, job):
        self.job, job.worker = job, self

    def pop_job(self):
        job, self.job = self.job, None
        if job:
            job.worker = None
        return job


class WorkerProtocol(NetstringReceiver, ProcessProtocol):

    def __init__(self, worker):
        self.__worker = worker

    def connectionMade(self):
        self.__worker.on_start(self.transport.pid)

    def outReceived(self, data):
        self.dataReceived(data)

    def stringReceived(self, payload):
        # Windows pipes use '\r\n', and both pickle and cPickle
        # can have trouble with that when unpickling (you may get
        # an 'insecure pickle string' message).
        if os.linesep != '\n':
            payload = payload.replace(os.linesep, '\n')
        self.__worker.on_result(pickle.loads(payload))

    def errReceived(self, data):
        self.__worker.on_log(data)

    def processEnded(self, reason):
        self.__worker.on_end(reason)


class MaybeLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def log(self, *args, **kw):
        self.logger and self.logger.log(*args, **kw)

    def debug(self, *args, **kw):
        self.logger and self.logger.debug(*args, **kw)

    def info(self, *args, **kw):
        self.logger and self.logger.info(*args, **kw)

    def warning(self, *args, **kw):
        self.logger and self.logger.warning(*args, **kw)

    def error(self, *args, **kw):
        self.logger and self.logger.error(*args, **kw)

    def critical(self, *args, **kw):
        self.logger and self.logger.critical(*args, **kw)


class Liner(object):
    """
    Perform "call(line)" for each complete line extracted from the added text.
    """

    def __init__(self, call):
        self.call = call
        self.remains = ''

    def add(self, text):
        lines = (self.remains + text).splitlines()
        self.remains = lines.pop(-1)

        for line in lines:
            self.call(line)

    def finish(self):
        remains, self.remains = self.remains, ''
        if remains:
            self.call(remains)


def make_deferred_with_timeout(canceller=None, timeout=None, timeout_msg=None,
                               **timeout_msg_kwargs):
    d = Deferred(canceller=canceller)

    if (timeout is not None) and (timeout >= 0):
        from twisted.internet import reactor

        timeout_msg_kwargs.update(timeout=timeout)

        if not timeout_msg:
            timeout_msg = 'User timeout of %(timeout)s caused failure.'

        timeout_msg %= timeout_msg_kwargs

        def on_timeout():
            if not d.called:
                d.errback(PoolTimeout(timeout_msg))

        timeout_call = reactor.callLater(timeout, on_timeout)

        def cancel_timeout(result):
            if timeout_call.active():
                timeout_call.cancel()
            return result

        d.addBoth(cancel_timeout)

    return d

