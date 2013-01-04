//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedDocumentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractReadTrigger))]
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