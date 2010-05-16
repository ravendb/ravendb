using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return;
            metadata[ReplicationConstants.RavenReplicationSource] = JToken.FromObject(Database.TransactionalStorage.Id);
        }
    }
}