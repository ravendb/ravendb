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
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10001)]
	[InheritedExport(typeof(AbstractAttachmentDeleteTrigger))]
	public class RemoveConflictOnAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
	{
		public override void OnDelete(string key)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var oldVersion = Database.GetStatic(key);
				if(oldVersion == null)
					return;

				if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
					return;

				var conflictData = oldVersion.Data().ToJObject();
				var conflicts = conflictData.Value<RavenJArray>("Conflicts");
				if(conflicts == null)
					return;
				foreach (var prop in conflicts)
				{
					Database.DeleteStatic(prop.Value<string>(), null);
				}
			}
		}
	}
}
