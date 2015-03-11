// -----------------------------------------------------------------------
//  <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractFilePutTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningPutTrigger : AbstractFilePutTrigger
	{
		private VersioningTriggerActions actions;

		public override void Initialize()
		{
			actions = new VersioningTriggerActions(FileSystem);
		}

		public override VetoResult AllowPut(string name, RavenJObject metadata)
		{
			return actions.AllowOperation(name, metadata);
		}

		public override void OnPut(string name, RavenJObject metadata)
		{
			actions.InitializeMetadata(name, metadata);
		}

		public override void AfterPut(string name, long? size, RavenJObject metadata)
		{
			actions.PutRevisionFile(name, size, metadata);
		}

		public override void OnUpload(string name, RavenJObject metadata, int pageId, int pagePositionInFile, int pageSize)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false) 
					return;

				object value;
				metadata.__ExternalState.TryGetValue("Next-Revision", out value);

				accessor.AssociatePage(name + "/revisions/" + value, pageId, pagePositionInFile, pageSize);
			});
		}

		public override void AfterUpload(string name, RavenJObject metadata)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false) 
					return;

				object value;
				metadata.__ExternalState.TryGetValue("Next-Revision", out value);

				var fileName = name + "/revisions/" + value;

				accessor.CompleteFileUpload(fileName);

				var currentMetadata = accessor.ReadFile(fileName).Metadata;
				currentMetadata["Content-MD5"] = metadata["Content-MD5"];

				accessor.UpdateFileMetadata(fileName, currentMetadata, null);
			});
		}

		public override void BeforeSynchronization(string name, RavenJObject metadata)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
					return;

				var revision = actions.GetNextRevisionNumber(name, accessor);

				metadata.__ExternalState["Synchronization-Next-Revision"] = revision;
			});
		}

		public override void AfterSynchronization(string name, string tempFile, RavenJObject metadata)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (actions.TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
					return;

				var revision = (long) metadata.__ExternalState["Synchronization-Next-Revision"];

				var tempFileRevision = string.Format("{0}/revisions/{1}", tempFile, revision);
				var fileRevision = string.Format("{0}/revisions/{1}", name, revision);

				accessor.RenameFile(tempFileRevision, fileRevision, true);

				actions.RemoveOldRevisions(name, revision, versioningConfiguration);
			});
		}		
	}
}