namespace Raven.Client.Documents.Operations
{
    public class CompareExchangeValue<T>
    {
        public readonly long Index;
        public readonly T Value;

        public CompareExchangeValue(long index, T value)
        {
            Index = index;
            Value = value;
        }
    }
}
