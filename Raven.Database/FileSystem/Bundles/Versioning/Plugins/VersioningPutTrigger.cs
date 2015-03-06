// -----------------------------------------------------------------------
//  <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
	[InheritedExport(typeof(AbstractFilePutTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningPutTrigger : AbstractFilePutTrigger
	{
		public override VetoResult AllowPut(string name, RavenJObject headers)
		{
			VetoResult veto = VetoResult.Allowed;
			FileSystem.Storage.Batch(accessor =>
			{
				var file = accessor.ReadFile(name);
				if (file == null)
					return;

				if (FileSystem.ChangesToRevisionsAllowed() == false &&
					file.Metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus) == "Historical" &&
					accessor.IsVersioningActive())
				{
					veto = VetoResult.Deny("Modifying a historical revision is not allowed");
				}
			});

			return veto;
		}

		public override void OnPut(string name, RavenJObject headers)
		{
			if (headers.ContainsKey(Constants.RavenCreateVersion))
			{
				headers.__ExternalState[Constants.RavenCreateVersion] = headers[Constants.RavenCreateVersion];
				headers.Remove(Constants.RavenCreateVersion);
			}

			if (headers.ContainsKey(Constants.RavenIgnoreVersioning))
			{
				headers.__ExternalState[Constants.RavenIgnoreVersioning] = headers[Constants.RavenIgnoreVersioning];
				headers.Remove(Constants.RavenIgnoreVersioning);
				return;
			}

			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (TryGetVersioningConfiguration(name, headers, accessor, out versioningConfiguration) == false) 
					return;

				var revision = GetNextRevisionNumber(name, accessor);

				using (FileSystem.DisableAllTriggersForCurrentThread())
				{
					RemoveOldRevisions(name, revision, versioningConfiguration);
				}

				headers.__ExternalState["Next-Revision"] = revision;
				headers.__ExternalState["Parent-Revision"] = headers.Value<string>(VersioningUtil.RavenFileRevision);

				headers[VersioningUtil.RavenFileRevisionStatus] = RavenJToken.FromObject("Current");
				headers[VersioningUtil.RavenFileRevision] = RavenJToken.FromObject(revision);
			});
		}

		public override void AfterPut(string name, long? size, RavenJObject headers)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (TryGetVersioningConfiguration(name, headers, accessor, out versioningConfiguration) == false) return;

				using (FileSystem.DisableAllTriggersForCurrentThread())
				{
					var copyHeaders = new RavenJObject(headers);
					copyHeaders[VersioningUtil.RavenFileRevisionStatus] = RavenJToken.FromObject("Historical");
					copyHeaders[Constants.RavenReadOnly] = true;
					copyHeaders.Remove(VersioningUtil.RavenFileRevision);
					object parentRevision;
					headers.__ExternalState.TryGetValue("Parent-Revision", out parentRevision);
					if (parentRevision != null)
					{
						copyHeaders[VersioningUtil.RavenFileParentRevision] = name + "/revisions/" + parentRevision;
					}

					object value;
					headers.__ExternalState.TryGetValue("Next-Revision", out value);

					accessor.PutFile(name + "/revisions/" + value, size, copyHeaders);
				}
			});
		}

		public override void OnUpload(string name, RavenJObject headers, int pageId, int pagePositionInFile, int pageSize)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (TryGetVersioningConfiguration(name, headers, accessor, out versioningConfiguration) == false) return;

				object value;
				headers.__ExternalState.TryGetValue("Next-Revision", out value);

				accessor.AssociatePage(name + "/revisions/" + value, pageId, pagePositionInFile, pageSize);
			});
		}

		public override void AfterUpload(string name, RavenJObject headers)
		{
			FileSystem.Storage.Batch(accessor =>
			{
				VersioningConfiguration versioningConfiguration;
				if (TryGetVersioningConfiguration(name, headers, accessor, out versioningConfiguration) == false) return;

				object value;
				headers.__ExternalState.TryGetValue("Next-Revision", out value);

				var fileName = name + "/revisions/" + value;

				accessor.CompleteFileUpload(fileName);

				var currentMetadata = accessor.ReadFile(fileName).Metadata;
				currentMetadata["Content-MD5"] = headers["Content-MD5"];

				accessor.UpdateFileMetadata(fileName, currentMetadata, null);
			});
		}

		private static long GetNextRevisionNumber(string name, IStorageActionsAccessor accessor)
		{
			long revision = 1;

			var existingFile = accessor.ReadFile(name);
			if (existingFile != null)
			{
				RavenJToken existingRevisionToken;
				if (existingFile.Metadata.TryGetValue(VersioningUtil.RavenFileRevision, out existingRevisionToken))
					revision = existingRevisionToken.Value<int>() + 1;
			}
			else
			{
				var latestRevisionsFile = GetLatestRevisionsFile(name, accessor);
				if (latestRevisionsFile != null)
				{
					var id = latestRevisionsFile.FullPath;
					if (id.StartsWith(name, StringComparison.CurrentCultureIgnoreCase))
					{
						var revisionNum = id.Substring((name + "/revisions/").Length);
						int result;
						if (int.TryParse(revisionNum, out result))
							revision = result + 1;
					}
				}
			}

			return revision;
		}

		private static FileHeader GetLatestRevisionsFile(string name, IStorageActionsAccessor accessor)
		{
			return accessor
				.GetFilesStartingWith(name + "/revisions/", 0, int.MaxValue)
				.LastOrDefault();
		}

		private void RemoveOldRevisions(string name, long revision, VersioningConfiguration versioningConfiguration)
		{
			var latestValidRevision = revision - versioningConfiguration.MaxRevisions;
			if (latestValidRevision <= 0)
				return;

			FileSystem.Files.IndicateFileToDelete(string.Format("{0}/revisions/{1}", name, latestValidRevision), null);
		}

		private static bool TryGetVersioningConfiguration(string name, RavenJObject metadata, IStorageActionsAccessor accessor, out VersioningConfiguration versioningConfiguration)
		{
			versioningConfiguration = null;
			if (name.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
				return false;

			if (metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus) == "Historical")
				return false;

			versioningConfiguration = accessor.GetVersioningConfiguration();
			if (versioningConfiguration == null || versioningConfiguration.Exclude
				|| (versioningConfiguration.ExcludeUnlessExplicit && !metadata.__ExternalState.ContainsKey(Constants.RavenCreateVersion))
				|| metadata.__ExternalState.ContainsKey(Constants.RavenIgnoreVersioning))
				return false;
			return true;
		}
	}
}