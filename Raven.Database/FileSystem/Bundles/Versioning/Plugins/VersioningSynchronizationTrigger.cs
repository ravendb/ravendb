// -----------------------------------------------------------------------
//  <copyright file="VersioningSynchronizationTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.FileSystem;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractSynchronizationTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningSynchronizationTrigger : AbstractSynchronizationTrigger
	{
		private VersioningTriggerActions actions;

		public override void Initialize()
		{
			actions = new VersioningTriggerActions(FileSystem);
		}

		public override void BeforeSynchronization(string name, RavenJObject metadata, SynchronizationType type)
		{
			if(type != SynchronizationType.ContentUpdate)
				return;

			FileSystem.Storage.Batch(accessor =>
			{
				FileVersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
					return;

				var revision = actions.GetNextRevisionNumber(name, accessor);

				metadata.__ExternalState["Synchronization-Next-Revision"] = revision;
			});
		}

		public override void AfterSynchronization(string name, RavenJObject metadata, SynchronizationType type, dynamic additionalData)
		{
			if (type != SynchronizationType.ContentUpdate)
				return;

			FileSystem.Storage.Batch(accessor =>
			{
				FileVersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
					return;

				var revision = (long) metadata.__ExternalState["Synchronization-Next-Revision"];

				var tempFileRevision = string.Format("{0}/revisions/{1}", additionalData.TempFileName, revision);
				var fileRevision = string.Format("{0}/revisions/{1}", name, revision);

				accessor.RenameFile(tempFileRevision, fileRevision, true);

				actions.RemoveOldRevisions(name, revision, versioningConfiguration);
			});
		}	
	}
}