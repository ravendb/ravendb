using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Json.Utilities
{
    public class CopyOnWriteJDictionary<TKey> : IDictionary<TKey, RavenJToken>, ICloneable
    {
        private static readonly RavenJToken DeletedMarker = new RavenJValue(null, JTokenType.Null);

        private IDictionary<TKey, RavenJToken> _localChanges;
        protected IDictionary<TKey, RavenJToken> LocalChanges { get { return _localChanges ?? (_localChanges = new Dictionary<TKey, RavenJToken>()); } }
        private CopyOnWriteJDictionary<TKey> _inherittedValues;

        public CopyOnWriteJDictionary()
        {
        }

        private CopyOnWriteJDictionary(IDictionary<TKey, RavenJToken> props)
        {
            _localChanges = props;
        }

        private CopyOnWriteJDictionary(CopyOnWriteJDictionary<TKey> previous)
        {
            _inherittedValues = previous;
        }

        #region Dictionary<TKey,TValue> Members

        public void Add(TKey key, RavenJToken value)
        {
            LocalChanges.Add(key, value);
        }

        public bool ContainsKey(TKey key)
        {
            return (_inherittedValues != null && _inherittedValues.ContainsKey(key)) ||
                (_localChanges != null && _localChanges.ContainsKey(key));
        }

        public ICollection<TKey> Keys
        {
            get
            {
                ICollection<TKey> keys = null;
                if (_inherittedValues != null)
                {
                    keys = _inherittedValues.Keys;
                }
                if (_localChanges != null)
                {
                    if (keys == null)
                        keys = _localChanges.Keys;
                    else
                    {
                        foreach (var key in  _localChanges.Keys)
                        {
                            keys.Add(key);
                        }
                    }
                }
                return keys;
            }
        }

        public bool Remove(TKey key)
        {
            LocalChanges[key] = DeletedMarker;
            return true;
        }

        public bool TryGetValue(TKey key, out RavenJToken value)
        {
            try
            {
                value = this[key];
                return true;
            }
            catch
            {
                value = null;
                return false;
            }
        }

        public ICollection<RavenJToken> Values
        {
            get { throw new NotImplementedException(); }
        }

        public RavenJToken this[TKey key]
        {
            get
            {
                RavenJToken val;
                if (_localChanges != null && _localChanges.TryGetValue(key, out val))
                    return val == DeletedMarker ? null : val;

                if (_inherittedValues != null && _inherittedValues.TryGetValue(key, out val))
                {
                    if (val == DeletedMarker)
                        return null;

                    // Will also perform a copy-on-write clone on object supporting this
                    var safeVal = val.CloneToken();
                    LocalChanges[key] = safeVal;
                    return safeVal;
                }
                return null;
            }
            set { LocalChanges[key] = value; }
        }

        #endregion

        #region Other not important operations
        public IEnumerator<KeyValuePair<TKey, RavenJToken>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, RavenJToken> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(KeyValuePair<TKey, RavenJToken> item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(KeyValuePair<TKey, RavenJToken>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<TKey, RavenJToken> item)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }
        #endregion

        #region ICloneable Members

        public object Clone()
        {
            if (_inherittedValues == null)
            {
                _inherittedValues = new CopyOnWriteJDictionary<TKey>(_localChanges);
                _localChanges = null;
                return new CopyOnWriteJDictionary<TKey>(_inherittedValues);
            }
            if (_localChanges == null)
            {
                return new CopyOnWriteJDictionary<TKey>(_inherittedValues);
            }
            _inherittedValues = new CopyOnWriteJDictionary<TKey>(
                new CopyOnWriteJDictionary<TKey>(_inherittedValues) { _localChanges = _localChanges });
            _localChanges = null;
            return new CopyOnWriteJDictionary<TKey>(_inherittedValues);
        }

        #endregion
    }
}
