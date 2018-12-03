using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreResult : SmugglerResult
    {
        public string DataDirectory { get; set; }

        public string JournalStoragePath { get; set; }
        
        public Counts SnapshotRestore { get; set; }

        public RestoreResult()
        {
            _progress = new RestoreProgress(this);
            SnapshotRestore = new Counts();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DataDirectory)] = DataDirectory;
            json[nameof(JournalStoragePath)] = DataDirectory;
            json[nameof(SnapshotRestore)] = SnapshotRestore.ToJson();
            return json;
        }
    }
}
