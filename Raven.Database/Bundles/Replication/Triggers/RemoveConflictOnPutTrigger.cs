//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractPutTrigger))]
	public class RemoveConflictOnPutTrigger : AbstractPutTrigger
	{
		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				metadata.Remove(Constants.RavenReplicationConflict);// you can't put conflicts

				var oldVersion = Database.Get(key, transactionInformation);
				if (oldVersion == null)
					return;
				if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
					return;

				RavenJArray history = metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
				metadata[Constants.RavenReplicationHistory] = history;

				var ravenJTokenEqualityComparer = new RavenJTokenEqualityComparer();
				// this is a conflict document, holding document keys in the 
				// values of the properties
				foreach (var prop in oldVersion.DataAsJson.Value<RavenJArray>("Conflicts"))
				{
					RavenJObject deletedMetadata;
					Database.Delete(prop.Value<string>(), null, transactionInformation, out deletedMetadata);

					// add the conflict history to the mix, so we make sure that we mark that we resolved the conflict
					var conflictHistory = deletedMetadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
					conflictHistory.Add(new RavenJObject
					{
						{Constants.RavenReplicationVersion, deletedMetadata[Constants.RavenReplicationVersion]},
						{Constants.RavenReplicationSource, deletedMetadata[Constants.RavenReplicationSource]}
					});

					foreach (var item in conflictHistory)
					{
						if(history.Any(x=>ravenJTokenEqualityComparer.Equals(x, item)))
							continue;
						history.Add(item);
					}
				}
			}
		}
	}
}
