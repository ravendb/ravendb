using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetConflictsResult
    {
        public Conflict[] Results { get; internal set; }

        public class Conflict
        {
            public string Key { get; set; }

            public ChangeVectorEntry[] ChangeVector { get; set; }

            public BlittableJsonReaderObject Doc { get; set; }
        }
    }
}
