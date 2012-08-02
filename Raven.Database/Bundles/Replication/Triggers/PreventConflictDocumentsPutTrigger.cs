// //-----------------------------------------------------------------------
// // <copyright file="PreventConflictDocumentsPutTrigger.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractPutTrigger))]
	public class PreventConflictDocumentsPutTrigger : AbstractPutTrigger
	{
		public override void OnPut(string key, Json.Linq.RavenJObject document, Json.Linq.RavenJObject metadata, Abstractions.Data.TransactionInformation transactionInformation)
		{
			metadata.Remove(Constants.RavenReplicationConflictDocument); // or conflict documents
		}

		public override VetoResult AllowPut(string key, Json.Linq.RavenJObject document, Json.Linq.RavenJObject metadata, Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
				return VetoResult.Deny("You cannot PUT a document with metadata " + Constants.RavenReplicationConflictDocument);
			JsonDocument documentByKey = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				documentByKey = accessor.Documents.DocumentByKey(key, transactionInformation);		
			});
			if (documentByKey == null)
				return VetoResult.Allowed;
			if (documentByKey.Metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
				return VetoResult.Deny("Conflict documents (with " + Constants.RavenReplicationConflictDocument +
					                ") are read only and can only be modified by RavenDB when you resolve the conflict");
			return VetoResult.Allowed;
		}
	}
}