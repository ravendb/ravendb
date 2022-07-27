using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class PoolWithLevels<TValue>
        where TValue : class, IDisposable, new()
    {
        public struct PooledValue : IDisposable
        {
            public TValue Value;
            private PoolWithLevels<TValue> _pool;

            public PooledValue(TValue value, PoolWithLevels<TValue> pool)
            {
                Value = value;
                _pool = pool;
                _pool?.IncrementLevel(value);
            }

            public void Dispose()
            {
                _pool?.DecrementLevel(Value);
                Value = null;
            }
        }

        public int ValueCount { get => _objectLevels.Count; }
        
        private readonly object _Lock = new();
        
        private int _maxCapacity;
        private int _targetLevel;
        private SortedList<int, HashSet<TValue>> _listByLevel = new();
        private Dictionary<TValue, int> _objectLevels = new();
        
        // maxCapacity is the maximum number of values to exist in the pool, it can not be exceeded
        // targetLevel is the target usage level of a pooled value:
        // first we try to use values up to this level before creating a new value
        // targetLevel can be exceeded in case all the values are used up to or over the target level
        public PoolWithLevels(int targetLevel, int maxCapacity)
        {
            _targetLevel = targetLevel > 0 ? targetLevel : -1;
            _maxCapacity = maxCapacity;
        }

        // for a new value request we always choose the value with the fill level that is the closest to the target one (below target are more preferrable)
        public PooledValue GetValue()
        {
            lock (_Lock)
            {
                TValue obj = null;
                if (_targetLevel > 0)
                {
                    // the value being the closest to the target from below should be selected
                    var level = _targetLevel - 1;
                    do
                    {
                        if (_listByLevel.TryGetValue(level, out var set))
                        {
                            if (set.Count >= 1)
                            {
                                obj = set.First();
                            }
                        }
                        level--;
                    } while (obj == null && level >= 0);
                }

                if (obj == null)
                {
                    // the value with the minimum level is selected
                    using (var it = _listByLevel.GetEnumerator())
                    {
                        while (it.MoveNext())
                        {
                            var (level, set) = it.Current;
                            if (set.Count >= 1)
                            {
                                obj = (_targetLevel > 0 && level >= _targetLevel && ValueCount < _maxCapacity) ? new TValue() : set.First();
                            }
                        }
                    }
                }
                
                if (obj == null)
                {
                    obj = new TValue();
                }

                return new PooledValue(obj, this);
            }
        }

        public void IncrementLevel(TValue obj)
        {
            ModifyLevel(obj, 1);
        }
        
        public void DecrementLevel(TValue obj)
        {
            lock (_Lock)
            {
                ModifyLevel(obj, -1);
            }
        }
        
        private void ModifyLevel(TValue obj, int delta)
        {
            if (delta == 0)
                return;

            var level = _objectLevels.ContainsKey(obj) ? _objectLevels[obj] : 0;

            if (_listByLevel.TryGetValue(level, out HashSet<TValue> setPrev))
            {
                setPrev.Remove(obj);
                // we don't remove the empty set as it will be used later and it is one per level on the whole raven server
            }

            int levelNew = level + delta;

            if (levelNew == 0 && ValueCount > 1)
            {
                // we remove the values that are not used any more, but keep at least one
                _objectLevels.Remove(obj);
                obj.Dispose();
            }
            else
            {
                _objectLevels[obj] = levelNew;
                if (!_listByLevel.TryGetValue(levelNew, out HashSet<TValue> setNew))
                {
                    setNew = new HashSet<TValue>();
                    _listByLevel[levelNew] = setNew;
                }

                setNew.Add(obj);
            }
        }
    }
}
