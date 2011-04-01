//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedDocumentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
    public class HideVirtuallyDeletedDocumentsReadTrigger : AbstractReadTrigger
    {
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation,
                                                 TransactionInformation transactionInformation)
        {
			if(metadata == null)
				return ReadVetoResult.Allowed; // this is a projection, it is allowed
            if (ReplicationContext.IsInReplicationContext)
                return ReadVetoResult.Allowed;
            RavenJToken value;
            if (metadata.TryGetValue("Raven-Delete-Marker", out value))
                return ReadVetoResult.Ignore;
            return ReadVetoResult.Allowed;
        }
    }
}
