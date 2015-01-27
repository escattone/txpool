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
import sys
import cPickle as pickle

try:
    import fcntl
except ImportError:
    fcntl = None


def set_close_on_exec(fd):
    """Set the close-on-exec flag for a file descriptor."""

    if fcntl is None:
        return

    old_flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, old_flags | fcntl.FD_CLOEXEC)


def isolate_io():
    """
    Isolates the original stdin and stdout, which
    are used to get jobs and return results, from
    the job's code.

    This prevents the job's code from corrupting
    the communication with the parent process.

    Return file-like objects open to the original
    stdin and stdout.
    """

    # Make a copy of stdin, and when on linux,
    # ensure it is closed for children of this
    # process.
    in_safe_fd = os.dup(sys.stdin.fileno())
    set_close_on_exec(in_safe_fd)
    in_safe = os.fdopen(in_safe_fd, 'rb')

    # Set stdin to read from os.devnull.
    with open(os.devnull, 'rb') as new_stdin:
        os.dup2(new_stdin.fileno(), sys.stdin.fileno())

    # Make a copy of stdout, and when on linux,
    # ensure it is closed for children of this
    # process.
    out_safe_fd = os.dup(sys.stdout.fileno())
    set_close_on_exec(out_safe_fd)
    out_safe = os.fdopen(out_safe_fd, 'wb', 0)

    # Set stdout to stderr.
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr

    return in_safe, out_safe


def import_callable(path):

    try:
        module_name, name = path.rsplit('.', 1)
    except ValueError:
        raise Exception('unable to resolve "%s" to a module and its callable '
                        'attribute' % path)

    __import__(module_name)
    module = sys.modules[module_name]

    call = getattr(module, name)

    if not callable(call):
        raise Exception('"%s" is not callable' % path)

    return call


def main():
    """
    Continually do the following:

    (1) from the standard input that has been isolated from the job's code,
        read a callable (or string used to import the callable) and its
        positional and keyword arguments.

    (2) make the call "callable(*args, **kw)" and write its pickled result to
        the standard output which has been isolated from the job's code.
    """

    in_safe, out_safe = isolate_io()

    while True:
        try:
            obj = pickle.load(in_safe)
        except KeyboardInterrupt:
            break

        if obj is None:
            print 'Stopping'
            break

        if isinstance(obj, basestring):
            call = import_callable(obj)
        elif callable(obj):
            call = obj
        else:
            args, kw = obj
            result = call(*(args or ()), **(kw or {}))
            pickled = pickle.dumps(result)
            out_safe.write('%d:%s,' % (len(pickled), pickled))
            out_safe.flush()

if __name__ == '__main__':
    main()
