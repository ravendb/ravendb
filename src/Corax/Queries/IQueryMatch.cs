namespace Corax.Queries
{
    public static class QueryMatch
    {
        public const long Invalid = -1;
        public const long Start = 0;
    }

    public interface IQueryMatch
    {
        long Count { get; }
        long Current { get; }

        bool SeekTo(long next = 0);
        bool MoveNext(out long v);
    }
}
