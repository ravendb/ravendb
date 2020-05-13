using Raven.Client.Documents.Session;
using Raven.Client.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class CompareExchangeValue<T> : ICompareExchangeValue
    {
        public string Key { get; }
        public long Index { get; internal set; }
        public T Value { get; set; }
        public IMetadataDictionary Metadata { get; set; }

        string ICompareExchangeValue.Key => Key;

        long ICompareExchangeValue.Index { get => Index; set => Index = value; }

        object ICompareExchangeValue.Value => Value;

        IMetadataDictionary ICompareExchangeValue.Metadata => Metadata;

        public CompareExchangeValue(string key, long index, T value, IMetadataDictionary metadata = null)
        {
            Key = key;
            Index = index;
            Value = value;
            Metadata = metadata ?? new MetadataAsDictionary();
        }
    }

    internal interface ICompareExchangeValue
    {
        public string Key { get; }
        public long Index { get; internal set; }
        public object Value { get; }
        public IMetadataDictionary Metadata { get; }
    }
}
