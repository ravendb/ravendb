using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Primitives;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal class MetadataAsDictionary : IDictionary<string, StringValues>
    {
        private IDictionary<string, StringValues> _metadata;
        private readonly BlittableJsonReaderObject _source;

        public MetadataAsDictionary(BlittableJsonReaderObject metadata)
        {
            _source = metadata;
        }

        public void Init()
        {
            _metadata = new Dictionary<string, StringValues>();
            var indexes = _source.GetPropertiesByInsertionOrder();
            foreach (var index in indexes)
            {
                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                _source.GetPropertyByIndex(index, ref propDetails);
                _metadata[propDetails.Name] = ConvertValue(propDetails.Value);
            }
        }

        private StringValues ConvertValue(object value)
        {
            var arr = value as BlittableJsonReaderArray;
            if (arr != null)
            {
                var strs = new string[arr.Length];
                for (int i = 0; i < arr.Length; i++)
                {
                    strs[i] = arr[i]?.ToString();
                }
                return new StringValues(strs);
            }
            return new StringValues(value.ToString());
        }

        public StringValues this[string key]
        {
            get
            {
                if (_metadata != null)
                    return _metadata[key];
                object value;
                if (_source.TryGetMember(key, out value))
                    return ConvertValue(value);

                throw new KeyNotFoundException(key + " is not in the metadata");
            }

            set
            {
                if (_metadata == null)
                    Init();
                Debug.Assert(_metadata != null);
                _metadata[key] = value;
            }
        }

        public bool Changed => _metadata != null;

        public int Count => _metadata?.Count ?? _source.GetPropertiesByInsertionOrder().Length;

        public bool IsReadOnly => _metadata != null && _metadata.IsReadOnly;

        public ICollection<string> Keys => _metadata != null ? _metadata.Keys : _source.GetPropertyNames();

        public ICollection<StringValues> Values
        {
            get
            {
                if (_metadata != null)
                    return _metadata.Values;
                var values = new List<StringValues>();
                foreach (var prop in _source.GetPropertiesByInsertionOrder())
                {
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _source.GetPropertyByIndex(prop, ref propDetails);
                    values.Add(ConvertValue(propDetails));
                }
                return values;
            }
        }

        public void Add(KeyValuePair<string, StringValues> item)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.Add(item.Key, item.Value);
        }

        public void Add(string key, StringValues value)
        {
            if (_metadata == null)
                Init();

            Debug.Assert(_metadata != null);
            _metadata.Add(key, value);
        }

        public void Clear()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.Clear();
        }

        public bool Contains(KeyValuePair<string, StringValues> item)
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

        public void CopyTo(KeyValuePair<string, StringValues>[] array, int arrayIndex)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, StringValues>> GetEnumerator()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }

        public bool Remove(KeyValuePair<string, StringValues> item)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.Remove(item);
        }

        public bool Remove(string key)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.Remove(key);
        }

        public bool TryGetValue(string key, out StringValues value)
        {
            if (_metadata != null)
                return _metadata.TryGetValue(key, out value);

            object val;
            if (_source.TryGetMember(key, out val))
            {
                value = ConvertValue(val);
                return true;
            }
            value = default(StringValues);
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }
    }
}
