// -----------------------------------------------------------------------
//  <copyright file="CommonVersioningTriggerBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Storage;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
    public class VersioningTriggerActions
    {
        internal const string CreationOfHistoricalRevisionIsNotAllowed = "Creating a historical revision is not allowed";
        internal const string ModificationOfHistoricalRevisionIsNotAllowed = "Modifying a historical revision is not allowed";

        private readonly RavenFileSystem fileSystem;

        public VersioningTriggerActions(RavenFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        public VetoResult AllowOperation(string name, RavenJObject metadata)
        {
            VetoResult veto = VetoResult.Allowed;
            fileSystem.Storage.Batch(accessor =>
            {
                if (accessor.IsVersioningActive(name) == false)
                {
                    veto = VetoResult.Allowed;
                }
                else if (accessor.IsVersioningDisabledForImport(metadata))
                {
                    veto = VetoResult.Allowed;
                }
                else
                {
                    var file = accessor.ReadFile(name);

                    if (fileSystem.ChangesToRevisionsAllowed() == false &&
                         (file?.Metadata ?? metadata).Value<string>(VersioningUtil.RavenFileRevisionStatus) == "Historical")
                    {
                        veto = VetoResult.Deny(file == null ? CreationOfHistoricalRevisionIsNotAllowed : ModificationOfHistoricalRevisionIsNotAllowed);
                    }
                }
            });
            return veto;
        }

        public void InitializeMetadata(string name, RavenJObject metadata)
        {
            if (metadata.ContainsKey(Constants.RavenCreateVersion))
            {
                metadata.__ExternalState[Constants.RavenCreateVersion] = metadata[Constants.RavenCreateVersion];
                metadata.Remove(Constants.RavenCreateVersion);
            }

            if (metadata.ContainsKey(Constants.RavenIgnoreVersioning))
            {
                metadata.__ExternalState[Constants.RavenIgnoreVersioning] = metadata[Constants.RavenIgnoreVersioning];
                metadata.Remove(Constants.RavenIgnoreVersioning);
                return;
            }

            fileSystem.Storage.Batch(accessor =>
            {
                FileVersioningConfiguration versioningConfiguration;
                if (TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
                    return;

                long revision;

                if (metadata.__ExternalState.ContainsKey("Synchronization-Next-Revision"))
                    revision = (long) metadata.__ExternalState["Synchronization-Next-Revision"];
                else
                    revision = GetNextRevisionNumber(name, accessor);

                RemoveOldRevisions(name, revision, versioningConfiguration);

                metadata.__ExternalState["Next-Revision"] = revision;
                metadata.__ExternalState["Parent-Revision"] = metadata.Value<string>(VersioningUtil.RavenFileRevision);

                metadata[VersioningUtil.RavenFileRevisionStatus] = RavenJToken.FromObject("Current");
                metadata[VersioningUtil.RavenFileRevision] = RavenJToken.FromObject(revision);
            });
        }

        public string PutRevisionFile(string name, long? size, RavenJObject metadata)
        {
            string revisionFile = null;

            fileSystem.Storage.Batch(accessor =>
            {
                FileVersioningConfiguration versioningConfiguration;
                if (TryGetVersioningConfiguration(name, metadata, accessor, out versioningConfiguration) == false)
                    return;

                using (fileSystem.DisableAllTriggersForCurrentThread())
                {
                    var copyHeaders = new RavenJObject(metadata);
                    copyHeaders[VersioningUtil.RavenFileRevisionStatus] = RavenJToken.FromObject("Historical");
                    copyHeaders[Constants.RavenReadOnly] = true;
                    copyHeaders.Remove(VersioningUtil.RavenFileRevision);
                    object parentRevision;
                    metadata.__ExternalState.TryGetValue("Parent-Revision", out parentRevision);
                    if (parentRevision != null)
                    {
                        copyHeaders[VersioningUtil.RavenFileParentRevision] = name + "/revisions/" + parentRevision;
                    }

                    object value;
                    metadata.__ExternalState.TryGetValue("Next-Revision", out value);

                    revisionFile = name + "/revisions/" + value;
                    accessor.PutFile(revisionFile, size, copyHeaders);
                }
            });

            return revisionFile;
        }

        public bool TryGetVersioningConfiguration(string name, RavenJObject metadata, IStorageActionsAccessor accessor, out FileVersioningConfiguration versioningConfiguration)
        {
            versioningConfiguration = null;
            if (name.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                return false;

            if (metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus) == "Historical")
                return false;

            versioningConfiguration = accessor.GetVersioningConfiguration(name);
            if (versioningConfiguration == null || versioningConfiguration.Exclude
                || (versioningConfiguration.ExcludeUnlessExplicit && !metadata.__ExternalState.ContainsKey(Constants.RavenCreateVersion))
                || metadata.__ExternalState.ContainsKey(Constants.RavenIgnoreVersioning))
                return false;
            return true;
        }

        public void RemoveOldRevisions(string name, long revision, FileVersioningConfiguration versioningConfiguration)
        {
            var latestValidRevision = revision - versioningConfiguration.MaxRevisions;
            if (latestValidRevision <= 0)
                return;

            using (fileSystem.DisableAllTriggersForCurrentThread())
            {
                fileSystem.Files.IndicateFileToDelete(string.Format("{0}/revisions/{1}", name, latestValidRevision), null);
            }
        }

        public long GetNextRevisionNumber(string name, IStorageActionsAccessor accessor)
        {
            long revision = 1;
            int latestRevision;

            var existingFile = accessor.ReadFile(name);
            if (existingFile != null && existingFile.Metadata.ContainsKey(SynchronizationConstants.RavenDeleteMarker) == false)
            {
                RavenJToken existingRevisionToken;
                if (existingFile.Metadata.TryGetValue(VersioningUtil.RavenFileRevision, out existingRevisionToken))
                    revision = existingRevisionToken.Value<int>() + 1;
            }
            else if (TryGetLatestRevisionNumber(name, accessor, out latestRevision))
            {
                revision = latestRevision + 1;
            }

            return revision;
        }

        private bool TryGetLatestRevisionNumber(string name, IStorageActionsAccessor accessor, out int latestRevision)
        {
            int? latest = null;

            foreach (var file in accessor.GetFilesStartingWith(name + "/revisions/", 0, int.MaxValue))
            {
                var revisionNum = file.Name;
                int revision;
                if (int.TryParse(revisionNum, out revision))
                {
                    if (latest == null || revision > latest)
                        latest = revision;
                }
            }

            if (latest != null)
            {
                latestRevision = latest.Value;
                return true;
            }

            latestRevision = -1;
            return false;
        }
    }
}
