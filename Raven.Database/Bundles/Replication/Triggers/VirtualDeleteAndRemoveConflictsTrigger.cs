//-----------------------------------------------------------------------
// <copyright file="VirtualDeleteAndRemoveConflictsTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	/// <summary>
	/// We can't allow real deletes when using replication, because
	/// then we won't have any way to replicate the delete. Instead
	/// we allow the delete but don't do actual delete, we replace it 
	/// with a delete marker instead
	/// </summary>
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	public class VirtualDeleteAndRemoveConflictsTrigger : AbstractDeleteTrigger
	{
		readonly ThreadLocal<RavenJArray> deletedHistory = new ThreadLocal<RavenJArray>();

		internal ReplicationHiLo HiLo
		{
			get
			{
				return (ReplicationHiLo)Database.ExtensionsState.GetOrAdd(typeof(ReplicationHiLo).AssemblyQualifiedName, o => new ReplicationHiLo
				{
					Database = Database
				});
			}
		}

		public override void OnDelete(string key, TransactionInformation transactionInformation)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.GetDocumentMetadata(key, transactionInformation);

				if (document == null)
					return;

                JsonDocument docWithBody;
				if (IsConflictDocument(document, transactionInformation, out docWithBody))
				{

					HandleConflictedDocument(docWithBody, transactionInformation);
					return;
				}

				HandleDocument(document);
			}
		}


        private bool IsConflictDocument(JsonDocumentMetadata document, TransactionInformation transactionInformation, out JsonDocument docWithBody)
        {
            docWithBody = null;
            var conflict = document.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);
            if (conflict == null || conflict.Value<bool>() == false)
            {
                return false;
            }

            docWithBody = Database.Get(document.Key, transactionInformation);

            var conflicts = docWithBody.DataAsJson.Value<RavenJArray>("Conflicts");
            if (conflicts != null)
            {
                return false;
            }

            return true;
        }
        public override void AfterDelete(string key, TransactionInformation transactionInformation)
		{
			var metadata = new RavenJObject
			{
				{Constants.RavenDeleteMarker, true},
				{Constants.RavenReplicationHistory, deletedHistory.Value},
				{Constants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
				{Constants.RavenReplicationVersion, HiLo.NextId()}
			};
			deletedHistory.Value = null;

			Database.TransactionalStorage.Batch(accessor => 
				accessor.Lists.Set(Constants.RavenReplicationDocsTombstones, key, metadata, UuidType.Documents));
		}

		private void HandleConflictedDocument(JsonDocument document, TransactionInformation transactionInformation)
		{
			var conflicts = document.DataAsJson.Value<RavenJArray>("Conflicts");
			var currentSource = Database.TransactionalStorage.Id.ToString();

			foreach (var c in conflicts)
			{
                RavenJObject conflict;
                Database.Delete(c.Value<string>(), null, transactionInformation, out conflict);

				var conflictSource = conflict.Value<RavenJValue>(Constants.RavenReplicationSource).Value<string>();

				if (conflictSource != currentSource)
					continue;

				this.deletedHistory.Value = new RavenJArray
				{
					new RavenJObject
					{
						{ Constants.RavenReplicationVersion, conflict[Constants.RavenReplicationVersion] },
						{ Constants.RavenReplicationSource, conflict[Constants.RavenReplicationSource] }
					}
				};

				return;
			}
		}

		private void HandleDocument(JsonDocumentMetadata document)
		{
			deletedHistory.Value = new RavenJArray(ReplicationData.GetHistory(document.Metadata))
			{
				new RavenJObject
				{
					{Constants.RavenReplicationVersion, document.Metadata[Constants.RavenReplicationVersion]},
					{Constants.RavenReplicationSource, document.Metadata[Constants.RavenReplicationSource]}
				}
			};
		}

	}
}
