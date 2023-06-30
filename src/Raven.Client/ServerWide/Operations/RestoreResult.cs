using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreResult : SmugglerResult
    {
        public string DataDirectory { get; set; }

        public string JournalStoragePath { get; set; }
        
        public Counts SnapshotRestore { get; set; }

        public FileCounts Files { get; set; }

        public RestoreResult()
        {
            _progress = new RestoreProgress(this);
            SnapshotRestore = new Counts();
            Files = new FileCounts();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DataDirectory)] = DataDirectory;
            json[nameof(JournalStoragePath)] = JournalStoragePath;
            json[nameof(SnapshotRestore)] = SnapshotRestore.ToJson();
            json[nameof(Files)] = Files.ToJson();
            return json;
        }
    }
}
