using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreProgress: SmugglerResult.SmugglerProgress
    {
        public long RestoredFilesInSnapshotCount { get; set; }

        public RestoreProgress(RestoreResult result) : base((SmugglerResult)result)
        {
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RestoredFilesInSnapshotCount)] = RestoredFilesInSnapshotCount;
            return json;
        }
    }
}