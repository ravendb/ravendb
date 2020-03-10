using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterEntry
    {
        public readonly LazyStringValue DocumentId;

        public readonly LazyStringValue Name;

        public readonly long Etag;

        public CounterEntry(LazyStringValue documentId, LazyStringValue counterName, long etag)
        {
            DocumentId = documentId;
            Name = counterName;
            Etag = etag;
        }
    }
}
