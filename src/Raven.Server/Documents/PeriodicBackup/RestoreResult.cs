using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreResult : SmugglerResult
    {
        public long RestoredFilesInSnapshotCount { get; set; }

        public string DataDirectory { get; set; }

        public string JournalStoragePath { get; set; }

        public RestoreResult()
        {
            _progress = new RestoreProgress(this);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RestoredFilesInSnapshotCount)] = RestoredFilesInSnapshotCount;
            json[nameof(DataDirectory)] = DataDirectory;
            json[nameof(JournalStoragePath)] = DataDirectory;
            return json;
        }
    }
}