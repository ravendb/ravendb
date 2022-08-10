using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class MetadataAsDictionary : IMetadataDictionary
    {
        private readonly IMetadataDictionary _parent;
        private readonly string _parentKey;

        private IDictionary<string, object> _metadata;
        private readonly BlittableJsonReaderObject _source;
        private bool _hasChanges;

        internal MetadataAsDictionary(BlittableJsonReaderObject metadata, IMetadataDictionary parent, string parentKey)
            : this(metadata)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            _parentKey = parentKey ?? throw new ArgumentNullException(nameof(parentKey));
        }

        public MetadataAsDictionary(BlittableJsonReaderObject metadata)
        {
            _source = metadata;
        }

        public MetadataAsDictionary() : this(new Dictionary<string, object>())
        {
        }

        public MetadataAsDictionary(Dictionary<string, object> metadata)
        {
            _metadata = metadata;
        }

        private void Initialize(BlittableJsonReaderObject metadata)
        {
            _metadata = new Dictionary<string, object>();
            for (int i = 0; i < metadata.Count; i++)
            {
                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                metadata.GetPropertyByIndex(i, ref propDetails);
                _metadata[propDetails.Name] = ConvertValue(propDetails.Name, propDetails.Value);
            }

            if (_parent != null) // mark parent as dirty
                _parent[_parentKey] = this;
        }

        private object ConvertValue(string key, object value)
        {
            if (value == null)
                return null;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value.ToString();

            if (value is long l)
                return l;

            if (value is bool b)
                return b;

            if (value is LazyNumberValue doubleValue)
                return (double)doubleValue;

            if (value is BlittableJsonReaderObject obj)
            {
                var dictionary = new MetadataAsDictionary(obj, this, key);
                dictionary.Initialize(obj);
                return dictionary;
            }

            if (value is BlittableJsonReaderArray array)
            {
                var result = new object[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    result[i] = ConvertValue(key, array[i]);
                }
                return result;
            }

            throw new NotImplementedException("Implement support for numbers and more");
        }

        public object this[string key]
        {
            get
            {
                if (_metadata != null)
                    return _metadata[key];
                if (_source.TryGetMember(key, out var value))
                    return ConvertValue(key, value);

                throw new KeyNotFoundException(key + " is not in the metadata");
            }

            set
            {
                if (_metadata == null)
                    Initialize(_source);
                Debug.Assert(_metadata != null);
                if (_metadata.TryGetValue(key, out var currentValue) == false || (currentValue != null && currentValue.Equals(value) == false) || (currentValue == null && value != null))
                {
                    _metadata[key] = value;
                    _hasChanges = true;
                }
            }
        }

        internal static MetadataAsDictionary MaterializeFromBlittable(BlittableJsonReaderObject metadata)
        {
            var result = new MetadataAsDictionary((Dictionary<string, object>)null);
            result.Initialize(metadata);

            return result;
        }

        public bool Changed => _metadata != null && _hasChanges;

        public int Count => _metadata?.Count ?? _source.Count;

        public bool IsReadOnly => _metadata != null && _metadata.IsReadOnly;

        public ICollection<string> Keys => _metadata != null ? _metadata.Keys : _source.GetPropertyNames();

        public ICollection<object> Values
        {
            get
            {
                if (_metadata != null)
                    return _metadata.Values;
                var values = new List<object>();
                for (int i = 0; i < _source.Count; i++)
                {
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _source.GetPropertyByIndex(i, ref propDetails);
                    values.Add(ConvertValue(propDetails.Name, propDetails));
                }
                return values;
            }
        }

        public void Add(KeyValuePair<string, object> item)
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            _metadata.Add(item.Key, item.Value);
            _hasChanges = true;
        }

        public void Add(string key, object value)
        {
            if (_metadata == null)
                Initialize(_source);

            Debug.Assert(_metadata != null);
            _metadata.Add(key, value);
            _hasChanges = true;
        }

        public void Clear()
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            if (_metadata.Count > 0)
            {
                _metadata.Clear();
                _hasChanges = true;
            }

        }

        public bool Contains(KeyValuePair<string, object> item)
        {
            if (_metadata != null)
                return _metadata.Contains(item);

            return _source.TryGetMember(item.Key, out var value) && value.ToString().Equals(item.Value);
        }

        public bool ContainsKey(string key)
        {
            if (_metadata != null)
                return _metadata.ContainsKey(key);

            return _source.TryGetMember(key, out _);
        }

        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            _metadata.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }

        public bool Remove(KeyValuePair<string, object> item)
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            var result = _metadata.Remove(item);
            _hasChanges |= result;
            return result;
        }

        public bool Remove(string key)
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            var result = _metadata.Remove(key);
            _hasChanges |= result;
            return result;
        }

        public bool TryGetValue(string key, out object value)
        {
            if (_metadata != null)
                return _metadata.TryGetValue(key, out value);

            if (_source.TryGetMember(key, out var val))
            {
                value = ConvertValue(key, val);
                return true;
            }
            value = default(object);
            return false;
        }

        public bool TryGetValue(string key, out string value)
        {
            var result = TryGetValue(key, out object obj);
            value = (string)obj;
            return result;
        }

        public string GetString(string key)
        {
            var obj = this[key];
            return Convert.ToString(obj, CultureInfo.InvariantCulture);
        }

        public long GetLong(string key)
        {
            var obj = this[key];
            return Convert.ToInt64(obj, CultureInfo.InvariantCulture);
        }

        public bool GetBoolean(string key)
        {
            var obj = this[key];
            return Convert.ToBoolean(obj, CultureInfo.InvariantCulture);
        }

        public double GetDouble(string key)
        {
            var obj = this[key];
            return Convert.ToDouble(obj, CultureInfo.InvariantCulture);
        }

        public IMetadataDictionary GetObject(string key)
        {
            var obj = this[key];
            return (IMetadataDictionary)obj;
        }

        public IMetadataDictionary[] GetObjects(string key)
        {
            var obj = (object[])this[key];
            return obj.Cast<IMetadataDictionary>().ToArray();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_metadata == null)
                Initialize(_source);
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }
    }
}
