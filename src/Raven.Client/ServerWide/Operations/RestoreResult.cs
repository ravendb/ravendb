using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreResult : SmugglerResult
    {
        public string DataDirectory { get; set; }

        public string JournalStoragePath { get; set; }
        
        public Counts SnapshotRestore { get; set; }

        public FileCounts SmugglerRestore { get; set; }

        public RestoreResult()
        {
            _progress = new RestoreProgress(this);
            SnapshotRestore = new Counts();
            SmugglerRestore = new FileCounts();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DataDirectory)] = DataDirectory;
            json[nameof(JournalStoragePath)] = JournalStoragePath;
            json[nameof(SnapshotRestore)] = SnapshotRestore.ToJson();
            json[nameof(SmugglerRestore)] = SmugglerRestore.ToJson();
            return json;
        }
    }
}
