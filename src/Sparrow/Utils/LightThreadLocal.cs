using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Sparrow.Utils
{
    public class LightThreadLocal<T> : IDisposable
    {
        [ThreadStatic] private static CurrentThreadState _state;
        private ConcurrentDictionary<CurrentThreadState, T> _values = 
            new ConcurrentDictionary<CurrentThreadState, T>(ReferenceEqualityComparer<CurrentThreadState>.Default);
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

        public class CurrentThreadState
        {
            private readonly HashSet<WeakReferenceToLightThreadLocal> _parents 
                = new HashSet<WeakReferenceToLightThreadLocal>();

            private class WeakReferenceToLightThreadLocal : IEquatable<WeakReferenceToLightThreadLocal>
            {
                private readonly WeakReference<LightThreadLocal<T>> _weak;
                private readonly int _hashCode;

                public bool TryGetTarget(out LightThreadLocal<T> target)
                {
                    return _weak.TryGetTarget(out target);
                }

                public WeakReferenceToLightThreadLocal(LightThreadLocal<T> instance)
                {
                    _hashCode = instance.GetHashCode();
                    _weak = new WeakReference<LightThreadLocal<T>>(instance);
                }

                public bool Equals(WeakReferenceToLightThreadLocal other)
                {
                    if (ReferenceEquals(null, other)) return false;
                    if (ReferenceEquals(this, other)) return true;
                    if (_hashCode != other._hashCode)
                        return false;
                    if (_weak.TryGetTarget(out var x) == false ||
                       other._weak.TryGetTarget(out var y) == false)
                        return false;
                    return ReferenceEquals(x, y);
                }

                public override bool Equals(object obj)
                {
                    if (ReferenceEquals(null, obj)) return false;
                    if (ReferenceEquals(this, obj)) return true;
                    if (obj.GetType() != this.GetType()) return false;
                    return Equals((WeakReferenceToLightThreadLocal)obj);
                }

                public override int GetHashCode()
                {
                    return _hashCode;
                }
            }

            public void Register(LightThreadLocal<T> parent)
            {
                _parents.Add(new WeakReferenceToLightThreadLocal(parent));
            }

            ~CurrentThreadState()
            {
                foreach (var parent in _parents)
                {
                    if (parent.TryGetTarget(out var liveParent) == false)
                        continue;
                    var copy = liveParent._values;
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
            GC.SuppressFinalize(this);
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

        ~LightThreadLocal()
        {
            Dispose();
        }
    }
}
