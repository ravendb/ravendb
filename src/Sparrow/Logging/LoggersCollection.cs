using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Sparrow.Logging
{
    public class LoggersCollection : IEnumerable<(string, SwitchLogger)>, IDisposable
    {
        private readonly SwitchLogger _parent;
        private readonly ConcurrentDictionary<string, SwitchLogger> _loggers = new ConcurrentDictionary<string, SwitchLogger>(StringComparer.OrdinalIgnoreCase);

        public int Count => _loggers.Count;
        
        public LoggersCollection(LoggingSource collectionOfLoggers, SwitchLogger parent)
        {
            _parent = parent;
        }

        public bool TryGet(string name, out SwitchLogger value) => _loggers.TryGetValue(name, out value);
        public SwitchLogger GetOrAdd(string name)
        {
            return _loggers.GetOrAdd(name, _ =>
                {
                    var source = _parent.Source != null ? $"{_parent.Source}.{_parent.Name}" : _parent.Name;
                    return new SwitchLogger(parent: _parent, source, name);
                }
               );
        }

        public void TryRemove(string name)
        {
            if (_loggers.TryRemove(name, out var holder))
                holder.Dispose();
        }

        public void TryUpdateMode(string name, LogMode mode)
        {
            if(_loggers.TryGetValue(name, out var switchLogger))
                switchLogger.UpdateMode(mode);
        }

        public IEnumerator<(string, SwitchLogger)> GetEnumerator()
        {
            foreach (var keyValue in _loggers)
            {
                yield return (keyValue.Key, keyValue.Value);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose()
        {
            foreach (SwitchLogger logger in _loggers.Values.ToArray())
            {
                logger.Dispose();
            }
        }
    }
}
