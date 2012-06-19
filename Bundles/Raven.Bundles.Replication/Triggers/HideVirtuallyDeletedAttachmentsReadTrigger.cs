//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedAttachmentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractAttachmentReadTrigger))]
	public class HideVirtuallyDeletedAttachmentsReadTrigger : AbstractAttachmentReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, Stream data, RavenJObject metadata, ReadOperation operation)
		{
			RavenJToken value;
			if (metadata.TryGetValue("Raven-Delete-Marker", out value))
				return ReadVetoResult.Ignore;
			return ReadVetoResult.Allowed;
		}
	}
}