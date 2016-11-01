using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Client.Documents
{
    public class MetadataAsDictionary : IDictionary<string, string>
    {
        private IDictionary<string, string> _metadata;
        private readonly BlittableJsonReaderObject _source;

        public MetadataAsDictionary(BlittableJsonReaderObject metadata)
        {
            _source = metadata;
        }

        public void Init()
        {
            _metadata = new Dictionary<string, string>();
            var indexes = _source.GetPropertiesByInsertionOrder();
            foreach (var index in indexes)
            {
                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                _source.GetPropertyByIndex(index, ref propDetails);
                _metadata[propDetails.Name] = propDetails.Value.ToString();
            };
        }

        public string this[string key]
        {
            get
            {
                if (_metadata != null)
                    return _metadata[key];
                object value;
                if (_source.TryGetMember(key, out value))
                    return value.ToString();
                throw new KeyNotFoundException(key + "is not in the metadata");
            }

            set
            {
                if (_metadata == null)
                    Init();
                _metadata[key] = value;
            }
        }

        public bool Changed => (_metadata != null);

        public int Count => (_metadata != null) ? _metadata.Count : _source.GetPropertiesByInsertionOrder().Length;

        public bool IsReadOnly => (_metadata != null) && _metadata.IsReadOnly;

        public ICollection<string> Keys => (_metadata != null) ? _metadata.Keys : _source.GetPropertyNames();

        public ICollection<string> Values
        {
            get
            {
                if (_metadata != null)
                    return _metadata.Values;
                var values = new List<string>();
                foreach (var prop in _source.GetPropertiesByInsertionOrder())
                {
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _source.GetPropertyByIndex(prop, ref propDetails);
                    values.Add(propDetails.Value.ToString());
                }
                return values;
            }
        }

        public void Add(KeyValuePair<string, string> item)
        {
            if (_metadata == null)
                Init();
            _metadata.Add(item.Key, item.Value);
        }

        public void Add(string key, string value)
        {
            if (_metadata == null)
                Init();
            _metadata.Add(key, value);
        }

        public void Clear()
        {
            if (_metadata == null)
                Init();
            _metadata.Clear();
        }

        public bool Contains(KeyValuePair<string, string> item)
        {
            if (_metadata != null)
                return _metadata.Contains(item);

            object value;
            return _source.TryGetMember(item.Key, out value) && (value.ToString().Equals(item.Value));
        }

        public bool ContainsKey(string key)
        {
            if (_metadata != null)
                return _metadata.ContainsKey(key);

            object value;
            return _source.TryGetMember(key, out value);
        }

        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
        {
            if (_metadata == null)
                Init();
            _metadata.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            if (_metadata == null)
                Init();
            return _metadata.GetEnumerator();
        }

        public bool Remove(KeyValuePair<string, string> item)
        {
            if (_metadata != null)
                Init();
            return _metadata.Remove(item);
        }

        public bool Remove(string key)
        {
            if (_metadata == null)
                Init();
            return _metadata.Remove(key);
        }

        public bool TryGetValue(string key, out string value)
        {
            if (_metadata != null)
                return _metadata.TryGetValue(key, out value);

            object val;
            if (_source.TryGetMember(key, out val))
            {
                value = val.ToString();
                return true;
            }
            value = null;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_metadata == null)
                Init();
            return _metadata.GetEnumerator();
        }
    }
}
