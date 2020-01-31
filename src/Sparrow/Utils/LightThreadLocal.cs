using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow.Utils
{
    public class LightThreadLocal<T> : IDisposable
    {
        [ThreadStatic] private static CurrentThreadState _state;
        private ConcurrentDictionary<CurrentThreadState, T> _values = new ConcurrentDictionary<CurrentThreadState, T>(ReferenceComparer<CurrentThreadState>.Instance);
        private readonly Func<T> _generator;

        public LightThreadLocal(Func<T> generator)
        {
            _generator = generator;
        }

        public ICollection<T> Values => _values.Values;

        public bool IsValueCreated => _state != null && _values.ContainsKey(_state);

        public T Value
        {
            get
            {
                (_state ??= new CurrentThreadState()).Register(this);
                if (_values.TryGetValue(_state, out var v) == false &&
                    _generator != null)
                {
                    v = _generator();
                    _values[_state] = v;
                }
                return v;
            }
            set
            {
                (_state ??= new CurrentThreadState()).Register(this);
                _values[_state] = value;
            }
        }

        private sealed class ReferenceComparer<T2> : IEqualityComparer<T2>
        {
            public static readonly ReferenceComparer<T2> Instance = new ReferenceComparer<T2>();
            public bool Equals(T2 x, T2 y)
            {
                return ReferenceEquals(x, y);
            }

            public int GetHashCode(T2 obj)
            {
                return RuntimeHelpers.GetHashCode(obj);
            }
        }

        public class CurrentThreadState
        {
            private readonly HashSet<LightThreadLocal<T>> _parents = new HashSet<LightThreadLocal<T>>(ReferenceComparer<LightThreadLocal<T>>.Instance);
            public void Register(LightThreadLocal<T> parent)
            {
                _parents.Add(parent);
            }

            ~CurrentThreadState()
            {
                foreach (var parent in _parents)
                {
                    var copy = parent._values;
                    if (copy == null)
                        continue;
                    if (copy.TryRemove(this, out var value)
                        && value is IDisposable d)
                    {
                        d.Dispose();
                    }
                }
            }
        }
        public void Dispose()
        {
            var copy = _values;
            _values = null;
            while (copy.Count > 0)
            {
                foreach (var kvp in copy)
                {
                    if (copy.TryRemove(kvp.Key, out var item) &&
                        item is IDisposable d)
                    {
                        d.Dispose();
                    }
                }
            }
        }
    }
}
