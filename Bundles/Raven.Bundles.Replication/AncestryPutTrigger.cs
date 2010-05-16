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
            var oldVersion = Database.Get(key, transactionInformation);
            Guid? oldEtag = oldVersion != null ? oldVersion.Etag : (Guid?)null;
            ReplicationUtil.AddAncestry(oldEtag, metadata);

        }
    }
}