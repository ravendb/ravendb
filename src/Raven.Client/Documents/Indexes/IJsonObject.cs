using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public interface IJsonObject : IEnumerable<KeyValuePair<string, object>>
    {
        object this[string propertyName] { get; }

        T Value<T>(string propertyName);

        IEnumerable<T> Values<T>();

        public interface IMetadata
        {
            object this[string propertyName] { get; }

            T Value<T>(string propertyName);

            IEnumerable<T> Values<T>();
        }
    }
}
