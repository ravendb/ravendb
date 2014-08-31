// -----------------------------------------------------------------------
//  <copyright file="DocumentCollectionChangeHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
	public class DocumentCollectionChangeHandler : AbstractPutTrigger
	{
		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			Database.TransactionalStorage.Batch(accessor =>
			{
				var documentMetadataByKey = accessor.Documents.DocumentMetadataByKey(key);
				if (documentMetadataByKey == null || documentMetadataByKey.Metadata == null)
					return;
				var existingVal = documentMetadataByKey.Metadata.Value<string>(Constants.RavenEntityName);
				var newVal = metadata.Value<string>(Constants.RavenEntityName);
				if (existingVal == newVal)
					return;

				// the collection name changed, need to notify all old indexes about it.
				Database.Documents.DeleteDocumentFromIndexesForCollection(key, existingVal, accessor);
			});
		}
	}
}