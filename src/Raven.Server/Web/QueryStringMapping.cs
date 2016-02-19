using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Web
{
    public class QueryStringMapping<T, THandler> : IEnumerable<KeyValuePair<string, Action<T, string>>> where T : new()
    {
        private readonly Dictionary<string, Action<T, string, THandler>> _schema = new Dictionary<string, Action<T, string, THandler>>(StringComparer.OrdinalIgnoreCase);

        public void Add(string parameterName, Action<T, string, THandler> setKeyAction)
        {
            _schema.Add(parameterName, setKeyAction);
        }

        public IEnumerator<KeyValuePair<string, Action<T, string>>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T Build(THandler requestHandler)
        {
            T result = new T();

            foreach (var item in _schema)
            {
                item.Value(result, item.Key, requestHandler);
            }

            return result;
        }
    }
}