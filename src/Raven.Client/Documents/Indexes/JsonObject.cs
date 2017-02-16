using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Client.Documents.Indexes
{
    public class JsonObject : IEnumerable<KeyValuePair<string, object>>
    {
        private readonly JObject _json;

        public JsonObject(JObject json)
        {
            _json = json;
        }

        public object this[string propertyName] => _json[propertyName];

        public virtual T Value<T>(string propertyName)
        {
            return _json.Value<T>(propertyName);
        }

        public IEnumerable<T> Values<T>()
        {
            return _json.Values<T>();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _json.GetEnumerator();
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return new Enumerator(_json);
        }

        public class Metadata
        {
            private readonly JObject _json;

            public Metadata(JObject json)
            {
                _json = json;
            }

            public object this[string propertyName] => _json[propertyName];

            public virtual T Value<T>(string propertyName)
            {
                return _json.Value<T>(propertyName);
            }

            public IEnumerable<T> Values<T>()
            {
                return _json.Values<T>();
            }
        }

        private class Enumerator : IEnumerator<KeyValuePair<string, object>>
        {
            private readonly IEnumerator<KeyValuePair<string, JToken>> _inner;

            public Enumerator(JObject json)
            {
                _inner = json.GetEnumerator();
            }

            public bool MoveNext()
            {
                return _inner.MoveNext();
            }

            public void Reset()
            {
                _inner.Reset();
            }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _inner.Dispose();
            }

            public KeyValuePair<string, object> Current => new KeyValuePair<string, object>(_inner.Current.Key, _inner.Current.Value);
        }
    }
}