// -----------------------------------------------------------------------
//  <copyright file="VersioningRenameTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractFileRenameTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningRenameTrigger : AbstractFileRenameTrigger
	{
		private VersioningTriggerActions actions;

		public override void Initialize()
		{
			actions = new VersioningTriggerActions(FileSystem);
		}

		public override VetoResult AllowRename(string name, string newName)
		{
			return actions.AllowOperation(name, null);
		}

		public override void AfterRename(string name, string renamed, RavenJObject metadata)
		{
			using (FileSystem.DisableAllTriggersForCurrentThread())
			{
				FileSystem.Storage.Batch(accessor =>
				{
					FileVersioningConfiguration versioningConfiguration;
					if (actions.TryGetVersioningConfiguration(name, new RavenJObject(), accessor, out versioningConfiguration) == false)
						return;

					var revisions = accessor.GetFilesStartingWith(name + "/revisions/", 0, int.MaxValue).Where(file => file != null).ToArray();

					for (int i = 0; i < revisions.Length; i++)
					{
						var file = revisions[i];

						if (versioningConfiguration.ResetOnRename)
						{
							if (i == (revisions.Length - 1))
							{
								// reset file revision
								metadata[VersioningUtil.RavenFileRevision] = RavenJToken.FromObject(1);
								metadata.Remove(VersioningUtil.RavenFileParentRevision);
								accessor.UpdateFileMetadata(renamed, metadata, null);

								// rename last existing revision to [renamed]/revisions/1
								var revision = file.Name;
								accessor.RenameFile(string.Format("{0}/revisions/{1}", name, revision), string.Format("{0}/revisions/{1}", renamed, 1), true);
							}
							else
							{
								FileSystem.Files.IndicateFileToDelete(file.FullPath, null);
							}
						}
						else
						{
							var revision = file.Name;

							accessor.RenameFile(string.Format("{0}/revisions/{1}", name, revision), string.Format("{0}/revisions/{1}", renamed, revision), true);
						}
					}
				});
			}
		}
	}
}