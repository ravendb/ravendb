using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterEntry
    {
        public readonly LazyStringValue DocumentId;

        public readonly string Name;

        public readonly long Etag;

        public CounterEntry(LazyStringValue documentId, string counterName, long etag)
        {
            DocumentId = documentId;
            Name = counterName;
            Etag = etag;
        }
    }
}
