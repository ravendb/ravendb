// -----------------------------------------------------------------------
//  <copyright file="VersioningDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractFileDeleteTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningDeleteTrigger : AbstractFileDeleteTrigger
	{
		private VersioningTriggerActions actions;

		public override void Initialize()
		{
			actions = new VersioningTriggerActions(FileSystem);
		}

		public override VetoResult AllowDelete(string name)
		{
			var result = VetoResult.Allowed;
			FileSystem.Storage.Batch(accessor =>
			{
				var file = accessor.ReadFile(name);
				if (file == null)
					return;

				if (file.Metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus) != "Historical")
					return;

				if (FileSystem.ChangesToRevisionsAllowed() || accessor.IsVersioningActive(name) == false)
					return;

				var revisionPos = name.LastIndexOf("/revisions/", StringComparison.OrdinalIgnoreCase);
				if (revisionPos != -1)
				{
					var parentName = name.Remove(revisionPos);
					var parentDoc = accessor.ReadFile(parentName);
					if (parentDoc == null)
						return;
				}

				result = VetoResult.Deny("Deleting a historical revision is not allowed");
			});

			return result;
		}

		public override void AfterDelete(string name)
		{
			using (FileSystem.DisableAllTriggersForCurrentThread())
			{
				FileSystem.Storage.Batch(accessor =>
				{
					FileVersioningConfiguration versioningConfiguration;
					if (actions.TryGetVersioningConfiguration(name, new RavenJObject(), accessor, out versioningConfiguration))
					{
						foreach (var file in accessor.GetFilesStartingWith(name + "/revisions/", 0, int.MaxValue).Where(file => file != null))
						{
							if (versioningConfiguration != null && versioningConfiguration.PurgeOnDelete)
								FileSystem.Files.IndicateFileToDelete(file.FullPath, null);
							else
							{
								file.Metadata.Remove(Constants.RavenReadOnly);
								accessor.UpdateFileMetadata(file.FullPath, file.Metadata, null);
							}
						}
					}
				});
			}
		}
	}
}