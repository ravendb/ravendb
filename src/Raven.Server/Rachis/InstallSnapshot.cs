using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public sealed class InstallSnapshot
    {
        public long LastIncludedIndex { get; set; }

        public long LastIncludedTerm { get; set; }

        public BlittableJsonReaderObject Topology { get; set; } 
    }

    public sealed class InstallSnapshotResponse
    {
        public bool Done { get; set; }
        public long CurrentTerm { get; set; }
        public long LastLogIndex { get; set; }
    }
}