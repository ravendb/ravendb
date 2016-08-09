using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Database.Config;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationStrategy
    {
        private readonly SigGenerator sigGenerator;

        private readonly InMemoryRavenConfiguration configuration;

        private readonly ITransactionalStorage storage;

        public SynchronizationStrategy(ITransactionalStorage storage, SigGenerator sigGenerator, InMemoryRavenConfiguration configuration)
        {
            this.storage = storage;
            this.sigGenerator = sigGenerator;
            this.configuration = configuration;
        }

        public bool Filter(FileHeader file, Guid destinationId, IEnumerable<FileHeader> candidatesToSynchronization)
        {
            // prevent synchronization back to source
            if (file.Metadata.Value<Guid>(SynchronizationConstants.RavenSynchronizationSource) == destinationId)
                return false;

            if (file.FullPath.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
                return false;

            if (file.FullPath.EndsWith(RavenFileNameHelper.DeletingFileSuffix))
                return false;

            if (file.IsFileBeingUploadedOrUploadHasBeenBroken())
                return false;

            if (ExistsRenameTombstone(file.FullPath, candidatesToSynchronization))
                return false;

            if (file.FullPath.Contains("/revisions/"))
                return false;

            return true;
        }

        private static bool ExistsRenameTombstone(string name, IEnumerable<FileHeader> candidatesToSynchronization)
        {
            return
                candidatesToSynchronization.Any(
                    x =>
                    x.Metadata[SynchronizationConstants.RavenDeleteMarker] != null &&
                    x.Metadata.Value<string>(SynchronizationConstants.RavenRenameFile) == name);
        }

        public SynchronizationWorkItem DetermineWork(string file, RavenJObject localMetadata, RavenJObject destinationMetadata, string localServerUrl, out NoSyncReason reason)
        {
            reason = NoSyncReason.Unknown;

            if (localMetadata == null)
            {
                reason = NoSyncReason.SourceFileNotExist;
                return null;
            }

            if (destinationMetadata != null && destinationMetadata[SynchronizationConstants.RavenSynchronizationConflict] != null && destinationMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] == null)
            {
                reason = NoSyncReason.DestinationFileConflicted;
                return null;
            }

            if (localMetadata[SynchronizationConstants.RavenSynchronizationConflict] != null)
            {
                reason = NoSyncReason.SourceFileConflicted;
                return null;
            }

            if (localMetadata[SynchronizationConstants.RavenDeleteMarker] != null)
            {
                if (localMetadata.ContainsKey(SynchronizationConstants.RavenRenameFile))
                {
                    var rename = localMetadata.Value<string>(SynchronizationConstants.RavenRenameFile);

                    if (destinationMetadata != null)
                        return new RenameWorkItem(file, rename, localServerUrl, storage);

                    return new ContentUpdateWorkItem(rename, localServerUrl, storage, sigGenerator, configuration);
                    // we have a rename tombstone but file does not exists on destination
                }

                if (destinationMetadata == null)
                {
                    reason = NoSyncReason.NoNeedToDeleteNonExistigFile;
                    return null;
                }

                return new DeleteWorkItem(file, localServerUrl, storage);
            }

            if (destinationMetadata != null && Historian.IsDirectChildOfCurrent(localMetadata, destinationMetadata))
            {
                reason = NoSyncReason.ContainedInDestinationHistory;
                return null;
            }

            // file exists on dest and has the same content
            if (destinationMetadata != null && localMetadata.Value<string>("Content-MD5") == destinationMetadata.Value<string>("Content-MD5"))
            {
                // check metadata to detect if any synchronization is needed
                if (localMetadata.Keys.Except(new[] { Constants.MetadataEtagField, Constants.RavenLastModified, Constants.LastModified })
                                 .Any(key => !destinationMetadata.ContainsKey(key) || localMetadata[key] != destinationMetadata[key]))
                {
                    return new MetadataUpdateWorkItem(file, localServerUrl, destinationMetadata, storage);
                }

                reason = NoSyncReason.SameContentAndMetadata;

                return null; // the same content and metadata - no need to synchronize
            }

            return new ContentUpdateWorkItem(file, localServerUrl, storage, sigGenerator, configuration);
        }
    }
}
