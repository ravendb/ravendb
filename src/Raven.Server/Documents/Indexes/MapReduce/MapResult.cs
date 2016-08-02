using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapResult
    {
        public BlittableJsonReaderObject Data;

        public ulong ReduceKeyHash;

        public long Id;
    }
}