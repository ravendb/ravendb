using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
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
