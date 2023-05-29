using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Sparrow.Logging
{
    internal class LoggersCollection : IEnumerable<(string, SwitchLogger)>, IDisposable
    {
        private readonly SwitchLogger _parent;
        private readonly ConcurrentDictionary<string, Lazy<SwitchLogger>> _loggers = new ConcurrentDictionary<string, Lazy<SwitchLogger>>(StringComparer.OrdinalIgnoreCase);

        public LoggersCollection(SwitchLogger parent)
        {
            _parent = parent;
        }
        
        public bool TryGet(string name, out SwitchLogger value)
        {
            if (_loggers.TryGetValue(name, out var lazyValue))
            {
                value = lazyValue.Value;
                return true;
            }

            value = null;
            return false;
        }
        
        public SwitchLogger GetOrAdd(string name)
        {
            return _loggers.GetOrAdd(name, n =>
            {
                return new Lazy<SwitchLogger>(() =>
                {
                    var source = _parent.Source != null ? $"{_parent.Source}.{_parent.Name}" : _parent.Name;
                    return new SwitchLogger(parent: _parent, source, n);
                });
            }).Value;
        }

        public void TryRemove(string name)
        {
            if (_loggers.TryRemove(name, out var holder))
                holder.Value.Dispose();
        }

        public IEnumerator<(string, SwitchLogger)> GetEnumerator()
        {
            foreach (var keyValue in _loggers)
            {
                yield return (keyValue.Key, keyValue.Value.Value);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose()
        {
            foreach (var logger in _loggers.Values.ToArray())
            {
                logger.Value.Dispose();
            }
        }
    }
}
