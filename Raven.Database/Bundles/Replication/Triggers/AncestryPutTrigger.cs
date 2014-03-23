//-----------------------------------------------------------------------
// <copyright file="AncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractPutTrigger))]
	public class AncestryPutTrigger : AbstractPutTrigger
	{
		internal ReplicationHiLo HiLo
		{
			get
			{
				return (ReplicationHiLo)Database.ExtensionsState.GetOrAdd(typeof (ReplicationHiLo).AssemblyQualifiedName, o => new ReplicationHiLo
				{
					Database = Database
				});
			}
		}

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) && // we don't deal with system documents
				key.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false) // except for hilos
				return;
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var documentMetadata = GetDocumentMetadata(key);
				if (documentMetadata != null)
				{
					RavenJArray history = new RavenJArray(ReplicationData.GetHistory(documentMetadata));
					metadata[Constants.RavenReplicationHistory] = history;

					if (documentMetadata.ContainsKey(Constants.RavenReplicationVersion) && 
						documentMetadata.ContainsKey(Constants.RavenReplicationSource))
					{
						history.Add(new RavenJObject
						{
							{Constants.RavenReplicationVersion, documentMetadata[Constants.RavenReplicationVersion]},
							{Constants.RavenReplicationSource, documentMetadata[Constants.RavenReplicationSource]}
						});
					}

					while (history.Length > Constants.ChangeHistoryLength)
					{
						history.RemoveAt(0);
					}
				}

				metadata[Constants.RavenReplicationVersion] = RavenJToken.FromObject(HiLo.NextId());
				metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
			}
		}

		private RavenJObject GetDocumentMetadata(string key)
		{
			var doc = Database.Documents.GetDocumentMetadata(key, null);
            if (doc != null)
            {
                var doesNotExist = doc.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists); // occurs when in transaction

                if (doesNotExist == false)
                    return doc.Metadata;
            }

			RavenJObject result = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				var tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, key);
				if (tombstone == null)
					return;
				result = tombstone.Data;
				accessor.Lists.Remove(Constants.RavenReplicationDocsTombstones, key);
			});
			return result;
		}
	}
}
