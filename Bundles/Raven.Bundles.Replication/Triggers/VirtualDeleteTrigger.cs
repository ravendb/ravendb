//-----------------------------------------------------------------------
// <copyright file="VirtualDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
    /// <summary>
    /// We can't allow real deletes when using replication, because
    /// then we won't have any way to replicate the delete. Instead
    /// we allow the delete but don't do actual delete, we replace it 
    /// with a delete marker instead
    /// </summary>
    public class VirtualDeleteTrigger : AbstractDeleteTrigger
    {
        readonly ThreadLocal<RavenJToken> deletedSource = new ThreadLocal<RavenJToken>();
        readonly ThreadLocal<RavenJToken> deletedVersion = new ThreadLocal<RavenJToken>();

        public override void OnDelete(string key, TransactionInformation transactionInformation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return;

            var document = Database.Get(key, transactionInformation);
            if (document == null)
                return;
            deletedSource.Value = document.Metadata[ReplicationConstants.RavenReplicationSource];
            deletedVersion.Value = document.Metadata[ReplicationConstants.RavenReplicationVersion];
        }

        public override void AfterDelete(string key, TransactionInformation transactionInformation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return;
			var metadata = new RavenJObject
        	{
        		{"Raven-Delete-Marker", true},
        		{ReplicationConstants.RavenReplicationParentSource, deletedSource.Value},
        		{ReplicationConstants.RavenReplicationParentVersion, deletedVersion.Value}
        	};
            deletedVersion.Value = null;
            deletedSource.Value = null;
            Database.Put(key, null, new RavenJObject(), metadata,transactionInformation);
        }
    }
}
