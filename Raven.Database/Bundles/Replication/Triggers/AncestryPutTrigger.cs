//-----------------------------------------------------------------------
// <copyright file="AncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
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
				return (ReplicationHiLo)Database.ExtensionsState.GetOrAdd(typeof (ReplicationHiLo), o => new ReplicationHiLo
				{
					Database = Database
				});
			}
		}

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase) && // we don't deal with system documents
				key.StartsWith("Raven/Hilo/", StringComparison.InvariantCultureIgnoreCase) == false) // except for hilos
				return;
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var doc = Database.Get(key, null);
				if (doc != null)
				{
					var history = doc.Metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
					metadata[Constants.RavenReplicationHistory] = history;

					if (doc.Metadata.ContainsKey(Constants.RavenReplicationVersion) && 
						doc.Metadata.ContainsKey(Constants.RavenReplicationSource))
					{
						history.Add(new RavenJObject
						{
							{Constants.RavenReplicationVersion, doc.Metadata[Constants.RavenReplicationVersion]},
							{Constants.RavenReplicationSource, doc.Metadata[Constants.RavenReplicationSource]}
						});
					}

					if (history.Length > Constants.ChangeHistoryLength)
					{
						history.RemoveAt(0);
					}
				}
				metadata[Constants.RavenReplicationVersion] = RavenJToken.FromObject(HiLo.NextId());
				metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
			}
		}
	}
}
