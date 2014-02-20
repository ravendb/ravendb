//-----------------------------------------------------------------------
// <copyright file="AttachmentAncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicBackups.Triggers
{
    [ExportMetadata("Bundle", "PeriodicBackup")]
	[ExportMetadata("Order", 10001)]
	[InheritedExport(typeof(AbstractAttachmentPutTrigger))]
	public class AttachmentAncestryPutTrigger : AbstractAttachmentPutTrigger
	{
		public override void OnPut(string key, Stream data, RavenJObject metadata)
		{
			if (key.StartsWith("Raven/")) // we don't deal with system attachment
				return;
			using (Database.DisableAllTriggersForCurrentThread())
			{
                Database.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, key);
                    if (tombstone == null)
                        return;
                    accessor.Lists.Remove(Constants.RavenPeriodicBackupsAttachmentsTombstones, key);
                });
			}
		}
	}
}