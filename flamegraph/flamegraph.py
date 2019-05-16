import re
import sys
import time
import os.path
import argparse
import threading
import traceback
import collections
import atexit
import functools
import signal

def get_thread_name(ident):
    for th in threading.enumerate():
        if th.ident == ident:
            return th.getName()
    return str(ident) # couldn't find, return something useful anyways

_default_format = '%(fun)s@%(short_fname)s:%(line)s'
def default_format_entry(fi, fmt=_default_format):
    return fmt % fi._asdict()

FrameInfo = collections.namedtuple('FrameInfo', ['fname', 'short_fname', 'line', 'fun'])

def extract_frame_info(frame):
    for fn, ln, fun, text in traceback.extract_stack(frame)[1:]:
        short_fname = re.sub(r'.*/', '', fn)
        yield FrameInfo(fn, short_fname, ln, fun)

def create_flamegraph_entry(frame, format_entry, collapse_recursion=False):
    # [1:] to skip first frame which is in this program
    if collapse_recursion:
        ret = []
        last = None
        for fi in extract_frame_info(frame):
            if last != fi.fun:
                ret.append(format_entry(fi))
            last = fi.fun
        return ';'.join(ret)

    return ';'.join(format_entry(fi) for fi in extract_frame_info(frame))

class Profiler:
    def __init__(self, fd, interval, filter, format_entry, collapse_recursion=False):
        self._lock = threading.Lock()
        self._fd = fd
        self._written = False
        self._interval = interval
        self._format_entry = format_entry
        self._collapse_recursion = collapse_recursion
        if filter is not None:
            self._filter = re.compile(filter)
        else:
            self._filter = None

        self._stats = collections.defaultdict(int)

    def start(self):
        signal.signal(signal.SIGPROF, self.on_itimer)
        signal.setitimer(signal.ITIMER_PROF, self._interval, self._interval)
        atexit.register(self.stop)

    def on_itimer(self, signum, frame):
        entry = create_flamegraph_entry(frame, self._format_entry, self._collapse_recursion)
        if self._filter is None or self._filter.search(entry):
            with self._lock:
                self._stats[entry] += 1

    def _write_results(self):
        with self._lock:
            if self._written:
                return
            self._written = True
            for key in sorted(self._stats.keys()):
                self._fd.write('%s %d\n' % (key, self._stats[key]))
            self._fd.close()

    def num_frames(self, unique=False):
        if unique:
            return len(self._stats)
        else:
            return sum(self._stats.values())

    def stop(self):
        signal.setitimer(signal.ITIMER_PROF, 0)
        self._write_results()

def start_profiler(fd, interval=0.001, filter=None, format_entry=default_format_entry, collapse_recursion=False):
    """Start a profiler thread."""
    profiler = Profiler(
        fd=fd,
        interval=interval,
        filter=filter,
        format_entry=format_entry,
        collapse_recursion=collapse_recursion)
    profiler.start()
    return profiler

def main():
    parser = argparse.ArgumentParser(prog='python -m flamegraph', description="Sample python stack frames for use with FlameGraph")
    parser.add_argument('script_file', metavar='script.py', type=str,
                        help='Script to profile')
    parser.add_argument('script_args', metavar='[arguments...]', type=str, nargs=argparse.REMAINDER,
                        help='Arguments for script')
    parser.add_argument('-o', '--output', nargs='?', type=argparse.FileType('w'), default=sys.stderr,
                        help='Save stats to file. If not specified default is to stderr')
    parser.add_argument('-i', '--interval', type=float, nargs='?', default=0.001,
                        help='Interval in seconds for collection of stackframes (default: %(default)ss)')
    parser.add_argument('-c', '--collapse-recursion', action='store_true',
                        help='Collapse simple recursion (function calls itself) into one stack frame in output')
    parser.add_argument('-f', '--filter', type=str, nargs='?', default=None,
                        help='Regular expression to filter which stack frames are profiled.  The '
                        'regular expression is run against each entire line of output so you can '
                        'filter by function or thread or both.')
    parser.add_argument('-F', '--format', type=str, nargs='?', default=_default_format,
                        help='Format-string (old-style) for encoding each stack frame into text.'
                        ' May include: "fn", "fun" and "line"')

    args = parser.parse_args()
    print(args)

    format_entry = functools.partial(default_format_entry, fmt=args.format)
    profiler = Profiler(args.output, args.interval, args.filter, format_entry, args.collapse_recursion)

    if not os.path.isfile(args.script_file):
        parser.error('Script file does not exist: ' + args.script_file)

    sys.argv = [args.script_file] + args.script_args
    sys.path.insert(0, os.path.dirname(args.script_file))
    script_compiled = compile(open(args.script_file, 'rb').read(), args.script_file, 'exec')
    script_globals = {'__name__': '__main__', '__file__': args.script_file, '__package__': None}

    start_time = time.clock()
    profiler.start()

    try:
        # exec docs say globals and locals should be same dictionary else treated as class context
        exec(script_compiled, script_globals, script_globals)
    finally:
        profiler.stop()
        print('Elapsed Time: %2.2f seconds.  Collected %d stack frames (%d unique)'
              % (time.clock() - start_time, profiler.num_frames(), profiler.num_frames(unique=True)))

if __name__ == '__main__':
    main()
