using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace Raven.Client.Json
{
    public sealed class JsonDynamicObject : DynamicObject, IDictionary<string, object>
    {
        private readonly Dictionary<string, object> _properties = new();

        public object this[string key]
        {
            get => _properties.TryGetValue(key, out object value) ? value : null;
            set => _properties[key] = value;
        }

        ICollection<string> IDictionary<string, object>.Keys => _properties.Keys;

        ICollection<object> IDictionary<string, object>.Values => _properties.Values;

        int ICollection<KeyValuePair<string, object>>.Count => _properties.Count;

        bool ICollection<KeyValuePair<string, object>>.IsReadOnly => false;

        void IDictionary<string, object>.Add(string key, object value) => _properties.Add(key, value);

        bool IDictionary<string, object>.ContainsKey(string key) => _properties.ContainsKey(key);

        bool IDictionary<string, object>.Remove(string key) => _properties.Remove(key);

        bool IDictionary<string, object>.TryGetValue(string key, out object value) => _properties.TryGetValue(key, out value);

        void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item) => _properties.Add(item.Key, item.Value);

        void ICollection<KeyValuePair<string, object>>.Clear() => _properties.Clear();

        bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item) => ((ICollection<KeyValuePair<string, object>>)_properties).Contains(item);

        void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex) =>
            ((ICollection<KeyValuePair<string, object>>)_properties).CopyTo(array, arrayIndex);

        bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item) => ((ICollection<KeyValuePair<string, object>>)_properties).Remove(item);

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator() => _properties.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _properties.GetEnumerator();

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = this[binder.Name];
            return true;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            this[binder.Name] = value;
            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            if (indexes.Length == 1 && indexes[0] is string key)
            {
                result = this[key];
                return true;
            }

            return base.TryGetIndex(binder, indexes, out result);
        }

        public override bool TrySetIndex(SetIndexBinder binder, object[] indexes, object value)
        {
            if (indexes.Length == 1 && indexes[0] is string key)
            {
                this[key] = value;
                return true;
            }

            return base.TrySetIndex(binder, indexes, value);
        }

        public override IEnumerable<string> GetDynamicMemberNames() => _properties.Keys;

        public override string ToString() => JsonDynamicFormatter.ToJson(this);
    }
}
