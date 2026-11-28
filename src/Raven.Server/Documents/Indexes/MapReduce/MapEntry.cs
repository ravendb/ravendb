namespace Raven.Server.Documents.Indexes.MapReduce
{
    public struct MapEntry
    {
        public ulong ReduceKeyHash;
        public long Id;
    }
}