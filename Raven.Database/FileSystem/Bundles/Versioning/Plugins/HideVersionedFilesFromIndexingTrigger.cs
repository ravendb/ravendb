// -----------------------------------------------------------------------
//  <copyright file="HideVersionedFilesFromIndexingTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Database.FileSystem.Plugins;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractFileReadTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class HideVersionedFilesFromIndexingTrigger : AbstractFileReadTrigger
	{
		public override ReadVetoResult AllowRead(string name, RavenJObject metadata, ReadOperation operation)
		{
			if (operation != ReadOperation.Index)
				return ReadVetoResult.Allowed;

			if (metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus) == "Historical" && FileSystem.IsVersioningActive(name))
				return ReadVetoResult.Ignore;

			return ReadVetoResult.Allowed;
		}
	}
}