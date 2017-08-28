using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public class LogSummary
    {
        public long CommitIndex { get; set; }
        public long LastTruncatedIndex { get; set; }
        public long LastTruncatedTerm { get; set; }
        public long FirstEntryIndex { get; set; }
        public long LastLogEntryIndex { get; set; }
        public List<BlittableJsonReaderObject> Entries { get; set; }
    }
}
