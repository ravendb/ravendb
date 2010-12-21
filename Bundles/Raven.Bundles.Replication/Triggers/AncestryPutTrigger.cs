//-----------------------------------------------------------------------
// <copyright file="AncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.Replication.Triggers
{
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        private ReplicationHiLo hiLo;
        public override void Initialize()
        {
            base.Initialize();
            hiLo = new ReplicationHiLo
            {
                Database = Database
            };
        }
        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/")) // we don't deal with system documents
                return;
            if (ReplicationContext.IsInReplicationContext)
                return;
            var doc = Database.Get(key, null);
            if (doc != null)
            {
                metadata[ReplicationConstants.RavenReplicationParentVersion] =
                    doc.Metadata[ReplicationConstants.RavenReplicationVersion];
                metadata[ReplicationConstants.RavenReplicationParentSource] =
                doc.Metadata[ReplicationConstants.RavenReplicationSource];
            }
            metadata[ReplicationConstants.RavenReplicationVersion] = JToken.FromObject(hiLo.NextId());
            metadata[ReplicationConstants.RavenReplicationSource] = JToken.FromObject(Database.TransactionalStorage.Id);
        }
    }
}
