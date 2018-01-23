namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class CompareExchangeValue<T>
    {
        public readonly string Key;
        public readonly long Index;
        public readonly T Value;

        public CompareExchangeValue(string key, long index, T value)
        {
            Key = key;
            Index = index;
            Value = value;
        }
    }
}
