//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedDocumentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Order", 10000)]
	public class HideVirtuallyDeletedDocumentsReadTrigger : AbstractReadTrigger
    {
		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation,
                                                 TransactionInformation transactionInformation)
        {
			if(metadata == null)
				return ReadVetoResult.Allowed; // this is a projection, it is allowed
            RavenJToken value;
            if (metadata.TryGetValue("Raven-Delete-Marker", out value))
                return ReadVetoResult.Ignore;
            return ReadVetoResult.Allowed;
        }
    }
}