//-----------------------------------------------------------------------
// <copyright file="VirtualAttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicBackups.Triggers
{
    [ExportMetadata("Bundle", "PeriodicBackup")]
	[ExportMetadata("Order", 10001)]
	[InheritedExport(typeof(AbstractAttachmentDeleteTrigger))]
	public class VirtualAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
	{
		public override void AfterDelete(string key)
		{
            var metadata = new RavenJObject
			{
				{Constants.RavenDeleteMarker, true},
			};

            Database.TransactionalStorage.Batch(accessor =>
                accessor.Lists.Set(Constants.RavenPeriodicBackupsAttachmentsTombstones, key, metadata, UuidType.Attachments));
		}
	}
}