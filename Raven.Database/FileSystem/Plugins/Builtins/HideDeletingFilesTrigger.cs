// -----------------------------------------------------------------------
//  <copyright file="HideVersionedFilesFromIndexingTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.FileSystem;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins.Builtins
{
	[InheritedExport(typeof(AbstractFileReadTrigger))]
	public class HideDeletingFilesTrigger : AbstractFileReadTrigger
	{
		public override ReadVetoResult AllowRead(string name, RavenJObject metadata, ReadOperation operation)
		{
			if (metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker))
				return ReadVetoResult.Ignore;

			return ReadVetoResult.Allowed;
		}
	}
}