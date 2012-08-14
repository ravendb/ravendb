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
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	public class RemoveConflictOnDeleteTrigger : AbstractDeleteTrigger
	{
		public override void OnDelete(string key, TransactionInformation transactionInformation)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var oldVersion = Database.Get(key, transactionInformation);
				if(oldVersion == null)
					return;

				if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
					return;

				// this is a conflict document, holding document keys in the  values of the properties
				foreach (var prop in oldVersion.DataAsJson.Value<RavenJArray>("Conflicts"))
				{
					RavenJObject deletedMetadata;
					Database.Delete(prop.Value<string>(), null, transactionInformation, out deletedMetadata);
				}
			}
		}
	}
}
