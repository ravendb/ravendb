//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnAttachmentPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Catalogs;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractAttachmentPutTrigger))]
	public class RemoveConflictOnAttachmentPutTrigger : AbstractAttachmentPutTrigger
	{
		public override void OnPut(string key, Stream data, RavenJObject metadata)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				metadata.Remove(Constants.RavenReplicationConflict);// you can't put conflicts

				var oldVersion = Database.GetStatic(key);
				if (oldVersion == null)
					return;
				if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
					return;

				RavenJArray history = metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
				metadata[Constants.RavenReplicationHistory] = history;

				var ravenJTokenEqualityComparer = new RavenJTokenEqualityComparer();
				// this is a conflict document, holding document keys in the 
				// values of the properties
				foreach (var prop in oldVersion.Metadata.Value<RavenJArray>("Conflicts"))
				{
					var id = prop.Value<string>();
					Attachment attachment = Database.GetStatic(id);
					if(attachment == null)
						continue;
					Database.DeleteStatic(id, null);

					// add the conflict history to the mix, so we make sure that we mark that we resolved the conflict
					var conflictHistory = attachment.Metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
					conflictHistory.Add(new RavenJObject
					{
						{Constants.RavenReplicationVersion, attachment.Metadata[Constants.RavenReplicationVersion]},
						{Constants.RavenReplicationSource, attachment.Metadata[Constants.RavenReplicationSource]}
					});

					foreach (var item in conflictHistory)
					{
						if (history.Any(x => ravenJTokenEqualityComparer.Equals(x, item)))
							continue;
						history.Add(item);
					}
				}
			}
		}
	}
}