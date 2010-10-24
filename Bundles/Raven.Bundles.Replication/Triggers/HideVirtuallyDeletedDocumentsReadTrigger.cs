using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.Replication.Triggers
{
    public class HideVirtuallyDeletedDocumentsReadTrigger : AbstractReadTrigger
    {
        public override ReadVetoResult AllowRead(string key, JObject document, JObject metadata, ReadOperation operation,
                                                 TransactionInformation transactionInformation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return ReadVetoResult.Allowed;
            JToken value;
            if (metadata.TryGetValue("Raven-Delete-Marker", out value))
                return ReadVetoResult.Ignore;
            return ReadVetoResult.Allowed;
        }
    }
}
