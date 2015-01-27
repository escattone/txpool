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

import os
import re
import math
import random
import logging

import mock
import pytest

from twisted.internet.defer import gatherResults, CancelledError

import txpool.pool
from txpool import Pool, cpu_count
from txpool import PoolError, PoolTimeout


@pytest.inlineCallbacks
def test_pool_1():

    pool = Pool(name='test1')

    assert pool.name == 'test1'
    assert pool.size == cpu_count()

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    directory = (os.path.dirname(__file__),)

    dirs = (directory,) * 4
    calls = ('os.path.exists', 'os.path.isdir',
             'os.path.isfile', 'os.path.islink')

    results = yield gatherResults(map(pool.apply_async, calls, dirs))

    exists, isdir, isfile, islink = results

    assert exists is True
    assert isdir is True
    assert isfile is False
    assert islink is False

    try:
        result = yield pool.apply_async('math.sqrt', (-1,), timeout=5)
    except Exception, e:
        result = e
    assert isinstance(result, PoolError)

    try:
        result = yield pool.apply_async(os.path.isdir, directory, timeout=5)
    except Exception, e:
        result = e
    assert result is True

    try:
        result = yield pool.close(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_2():

    filename = 'test2.log'

    log = logging.getLogger('test2')
    log.addHandler(logging.FileHandler(filename, 'w'))
    log.setLevel(logging.DEBUG)

    pool = Pool(size=3, init_call='math.sqrt', init_args=(81,), log=log,
                name='test2')

    assert pool.size == 3
    assert pool.name == 'test2'

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    result = yield pool.apply_async('math.factorial', (9,), timeout=5)

    assert result == 362880

    try:
        result = yield pool.close(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0

    with open(filename, 'r') as f:
        log_text = f.read()

    pat1 = r'Pool "test2": process \d+ started\.'
    pat2 = r'Pool "test2": process \d+ initialized\.'
    pat3 = (r'Pool "test2" \[\d+\]: <Initializer object '
            'at 0x[0-9a-f]+: math\.sqrt\(81\)>: 9\.0')
    pat4 = (r'Pool "test2" \[\d+\]: <Job object '
            'at 0x[0-9a-f]+: math\.factorial\(9\)>: 362880')
    pat5 = r'Pool "test2": closing process \d+\.'
    pat6 = r'Pool "test2" \[\d+\]: Stopping'
    pat7 = (r'Pool "test2": process \d+ ended '
            '\(exit-code=0, signal=None\) while idle\.')

    assert len(re.findall(pat1, log_text)) == 3
    assert len(re.findall(pat2, log_text)) == 3
    assert len(re.findall(pat3, log_text)) == 3
    assert len(re.findall(pat4, log_text)) == 1
    assert len(re.findall(pat5, log_text)) == 3
    assert len(re.findall(pat6, log_text)) == 3
    assert len(re.findall(pat7, log_text)) == 3


@pytest.inlineCallbacks
def test_pool_3():

    pool = Pool(size=10, name='test3a')

    assert pool.size == 10
    assert pool.name == 'test3a'

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    try:
        result = yield pool.terminate(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0

    pool = Pool(size=10, name='test3b')

    assert pool.size == 10
    assert pool.name == 'test3b'

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    pool.terminate()

    try:
        result = yield pool.on_closure(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_4():

    pool = Pool(size=4, name='test4')

    assert pool.size == 4
    assert pool.name == 'test4'

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    deferreds = []
    for _ in xrange(100):
        deferreds.append(pool.apply_async('os.listdir', ('....',)))

    for d in deferreds:
        try:
            result = yield d
        except Exception, e:
            result = e
        assert isinstance(result, PoolError)

    try:
        result = yield pool.close(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_5():

    pool = Pool(size=4, name='test5')

    assert pool.size == 4
    assert pool.name == 'test5'

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    deferreds = []
    cancelleds = set()
    args = range(1000) + range(-200, 0)
    random.shuffle(args)

    for i, arg in enumerate(args):
        d = pool.apply_async(math.factorial, (arg,), timeout=30)
        deferreds.append((arg, d))
        if i in (100, 200, 300, 400, 500):
            d.cancel()
            cancelleds.add(arg)

    for arg, d in deferreds:
        try:
            result = yield d
        except Exception, e:
            result = e

        if arg in cancelleds:
            assert isinstance(result, CancelledError)
        elif arg >= 0:
            assert result == math.factorial(arg)
        else:
            assert isinstance(result, PoolError)

    try:
        result = yield pool.apply_async('time.sleep', (3,), timeout=1)
    except Exception, e:
        result = e
    assert isinstance(result, PoolTimeout)

    pool.close()

    try:
        result = yield pool.on_closure(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_6():

    pool = Pool(name='test6')

    assert pool.name == 'test6'
    assert pool.size == cpu_count()

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    d = pool.apply_async('math.sqrt', (16,), timeout=5)

    d.cancel()

    try:
        result = yield d
    except Exception, e:
        result = e
    assert isinstance(result, CancelledError)

    try:
        result = yield pool.close(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_7():

    pool = Pool(name='test7')

    assert pool.name == 'test7'
    assert pool.size == cpu_count()

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    # Try using keyword arguments.
    result = yield pool.apply_async(int, ('1010',), dict(base=2))
    assert result == 10

    pool.close()

    # Try "apply_async" after closing.
    try:
        result = yield pool.apply_async(int, ('1011',), dict(base=2))
    except Exception, e:
        result = e
    assert isinstance(result, PoolError)

    # Try "on_ready" after closing.
    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert isinstance(result, PoolError)

    try:
        result = yield pool.on_closure(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == 0

    # Call again after pool has closed.
    try:
        result = yield pool.on_closure()
    except Exception, e:
        result = e
    assert result is pool


def test_pool_8():
    try:
        pool = Pool(size=0, name='test8')
    except Exception, e:
        pool = e
    assert isinstance(pool, ValueError)

    try:
        pool = Pool(size=-10, name='test8')
    except Exception, e:
        pool = e
    assert isinstance(pool, ValueError)


@pytest.inlineCallbacks
def test_pool_9():

    pool = Pool(name='test9', init_call='math.sqrt', init_args=(-1,))

    assert pool.name == 'test9'
    assert pool.size == cpu_count()

    try:
        result = yield pool.on_ready(timeout=5)
    except Exception, e:
        result = e
    assert result is pool
    assert pool.get_number_of_workers() == pool.size

    # This will timeout as we haven't called "close".
    try:
        result = yield pool.on_closure(timeout=2)
    except Exception, e:
        result = e
    assert isinstance(result, PoolTimeout)

    # This will timeout.  Since the init_call's failed, the pool will
    # collapse (all of the processes will end due to an initialization
    # error and will not be replaced), and will never fire "on_ready"
    # again.
    try:
        result = yield pool.on_ready(timeout=2)
    except Exception, e:
        result = e
    assert isinstance(result, PoolTimeout)
    assert pool.get_number_of_workers() == 0


@pytest.inlineCallbacks
def test_pool_10():
    with mock.patch('txpool.pool.cpu_count', side_effect=NotImplementedError):
        pool = Pool(name='test10')
        assert pool.size == txpool.pool.DEFAULT_POOL_SIZE

        try:
            result = yield pool.on_ready(timeout=5)
        except Exception, e:
            result = e
        assert result is pool
        assert pool.get_number_of_workers() == pool.size
