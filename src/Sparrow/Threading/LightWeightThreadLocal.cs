using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Sparrow.Utils;

namespace Sparrow.Threading
{
    public class LightWeightThreadLocal<T> : IDisposable
    {
        [ThreadStatic] private static CurrentThreadState _state;
        private ConcurrentDictionary<CurrentThreadState, T> _values = new ConcurrentDictionary<CurrentThreadState, T>(ReferenceEqualityComparer<CurrentThreadState>.Default);
        private readonly Func<T> _generator;

        public LightWeightThreadLocal(Func<T> generator = null)
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

        public void Dispose()
        {
            if (_values == null)
                return;

            lock (this)
            {
                var copy = _values;
                if (copy == null)
                    return;

                GC.SuppressFinalize(this);

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

        ~LightWeightThreadLocal()
        {
            Dispose();
        }

        private class CurrentThreadState
        {
            private readonly HashSet<WeakReferenceToLightWeightThreadLocal> _parents
                = new HashSet<WeakReferenceToLightWeightThreadLocal>();

            private class WeakReferenceToLightWeightThreadLocal : IEquatable<WeakReferenceToLightWeightThreadLocal>
            {
                private readonly WeakReference<LightWeightThreadLocal<T>> _weak;
                private readonly int _hashCode;

                public bool TryGetTarget(out LightWeightThreadLocal<T> target)
                {
                    return _weak.TryGetTarget(out target);
                }

                public WeakReferenceToLightWeightThreadLocal(LightWeightThreadLocal<T> instance)
                {
                    _hashCode = instance.GetHashCode();
                    _weak = new WeakReference<LightWeightThreadLocal<T>>(instance);
                }

                public bool Equals(WeakReferenceToLightWeightThreadLocal other)
                {
                    if (ReferenceEquals(null, other))
                        return false;
                    if (ReferenceEquals(this, other))
                        return true;
                    if (_hashCode != other._hashCode)
                        return false;
                    if (_weak.TryGetTarget(out var x) == false ||
                       other._weak.TryGetTarget(out var y) == false)
                        return false;
                    return ReferenceEquals(x, y);
                }

                public override bool Equals(object obj)
                {
                    if (ReferenceEquals(null, obj))
                        return false;
                    if (ReferenceEquals(this, obj))
                        return true;
                    if (obj.GetType() != GetType())
                        return false;
                    return Equals((WeakReferenceToLightWeightThreadLocal)obj);
                }

                public override int GetHashCode()
                {
                    return _hashCode;
                }
            }

            public void Register(LightWeightThreadLocal<T> parent)
            {
                _parents.Add(new WeakReferenceToLightWeightThreadLocal(parent));
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
    }
}
