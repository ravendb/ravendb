using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Client.Documents
{
    public class MetadataAsDictionary : IDictionary<string, string>
    {
        private readonly Lazy<IDictionary<string, string>> _lazyMetadata;
        private readonly BlittableJsonReaderObject _source;

        public MetadataAsDictionary(BlittableJsonReaderObject metadata)
        {
            _source = metadata;
            _lazyMetadata = new Lazy<IDictionary<string, string>>(Init);
        }

        public IDictionary<string, string> Init()
        {
            var metadataAsDictionary = new Dictionary<string, string>();
            var indexes = _source.GetPropertiesByInsertionOrder();
            foreach (var index in indexes)
            {
                var prop = _source.GetPropertyByIndex(index);
                metadataAsDictionary[prop.Item1] = prop.Item2.ToString();
            };
            return metadataAsDictionary;
        }

        public string this[string key]
        {
            get
            {
                if (_lazyMetadata.IsValueCreated)
                    return _lazyMetadata.Value[key];
                object value;
                if (_source.TryGetMember(key, out value))
                    return value.ToString();
                throw new KeyNotFoundException(key + "is not in the metadata");
            }

            set
            {
                _lazyMetadata.Value[key] = value;
            }
        }

        public bool Changed => _lazyMetadata.IsValueCreated;

        public int Count => _lazyMetadata.IsValueCreated ? _lazyMetadata.Value.Count : _source.GetPropertiesByInsertionOrder().Length;

        public bool IsReadOnly => _lazyMetadata.IsValueCreated && _lazyMetadata.Value.IsReadOnly;

        public ICollection<string> Keys => _lazyMetadata.IsValueCreated ? _lazyMetadata.Value.Keys : _source.GetPropertyNames();

        public ICollection<string> Values
        {
            get
            {
                if (_lazyMetadata.IsValueCreated)
                    return _lazyMetadata.Value.Values;
                var values = new List<string>();
                foreach (var prop in _source.GetPropertiesByInsertionOrder())
                {
                    values.Add(_source.GetPropertyByIndex(prop).Item2.ToString());
                }
                return values;
            }
        }

        public void Add(KeyValuePair<string, string> item)
        {
            _lazyMetadata.Value.Add(item.Key, item.Value);
        }

        public void Add(string key, string value)
        {
            _lazyMetadata.Value.Add(key, value);
        }

        public void Clear()
        {
            _lazyMetadata.Value.Clear();
        }

        public bool Contains(KeyValuePair<string, string> item)
        {
            if (_lazyMetadata.IsValueCreated)
                return _lazyMetadata.Value.Contains(item);

            object value;
            return _source.TryGetMember(item.Key, out value) && (value.ToString().Equals(item.Value));
        }

        public bool ContainsKey(string key)
        {
            if (_lazyMetadata.IsValueCreated)
                return _lazyMetadata.Value.ContainsKey(key);

            object value;
            return _source.TryGetMember(key, out value);
        }

        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
        {
            _lazyMetadata.Value.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return _lazyMetadata.Value.GetEnumerator();
        }

        public bool Remove(KeyValuePair<string, string> item)
        {
            return _lazyMetadata.Value.Remove(item);
        }

        public bool Remove(string key)
        {
            return _lazyMetadata.Value.Remove(key);
        }

        public bool TryGetValue(string key, out string value)
        {
            if (_lazyMetadata.IsValueCreated)
                return _lazyMetadata.Value.TryGetValue(key, out value);

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
            return _lazyMetadata.Value.GetEnumerator();
        }
    }
}
