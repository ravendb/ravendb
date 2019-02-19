using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Logging;

namespace Sparrow.Server
{
    public class ContentionLoggingLocker
    {
        private readonly Logger _logger;
        private readonly string _name;
        private readonly object _locker = new object();
        private bool _lockTaken;

        public ContentionLoggingLocker(Logger logger, string name)
        {
            _logger = logger;
            _name = name;
        }

        public struct Release : IDisposable
        {
            private readonly ContentionLoggingLocker _parent;

            public Release(ContentionLoggingLocker parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                if (_parent._lockTaken)
                    Monitor.Exit(_parent._locker);
            }
        }

        public Release Lock([CallerMemberName] string caller = null, [CallerLineNumber] int line = 0)
        {
            _lockTaken = false;
            Monitor.TryEnter(_locker, 0, ref _lockTaken);
            if (_lockTaken == false)
            {
                var sp = Stopwatch.StartNew();
                Monitor.TryEnter(_locker, Timeout.Infinite, ref _lockTaken);
                Debug.Assert(_lockTaken);
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Contention on lock {_name} from {caller} : {line} for {sp.ElapsedMilliseconds:#,#;;0} ms");
                }
            }
            return new Release(this);
        }
    }
}
