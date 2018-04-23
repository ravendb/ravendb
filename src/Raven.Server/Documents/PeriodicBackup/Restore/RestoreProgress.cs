using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreProgress : SmugglerResult.SmugglerProgress
    {
        public Counts SnapshotRestore => ((RestoreResult)_result).SnapshotRestore;

        public RestoreProgress(RestoreResult result) : base(result)
        {
            
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SnapshotRestore)] = SnapshotRestore.ToJson();
            return json;
        }
    }
}
