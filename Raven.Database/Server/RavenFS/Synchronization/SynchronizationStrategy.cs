using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationStrategy
	{
		private readonly SigGenerator sigGenerator;
		private readonly ITransactionalStorage storage;

		public SynchronizationStrategy(ITransactionalStorage storage, SigGenerator sigGenerator)
		{
			this.storage = storage;
			this.sigGenerator = sigGenerator;
		}

		public bool Filter(FileHeader file, Guid destinationId, IEnumerable<FileHeader> candidatesToSynchronization)
		{
			// prevent synchronization back to source
			if (file.Metadata[SynchronizationConstants.RavenSynchronizationSource] == destinationId.ToString())
				return false;

			if (file.Name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
				return false;

			if (file.Name.EndsWith(RavenFileNameHelper.DeletingFileSuffix))
				return false;

			if (file.IsFileBeingUploadedOrUploadHasBeenBroken())
				return false;

			if (ExistsRenameTombstone(file.Name, candidatesToSynchronization))
				return false;

			return true;
		}

		private static bool ExistsRenameTombstone(string name, IEnumerable<FileHeader> candidatesToSynchronization)
		{
			return
				candidatesToSynchronization.Any(
					x =>
					x.Metadata[SynchronizationConstants.RavenDeleteMarker] != null &&
					x.Metadata[SynchronizationConstants.RavenRenameFile] == name);
		}

		public SynchronizationWorkItem DetermineWork(string file, NameValueCollection localMetadata,
													 NameValueCollection destinationMetadata, string localServerUrl,
													 out NoSyncReason reason)
		{
			reason = NoSyncReason.Unknown;

			if (localMetadata == null)
			{
				reason = NoSyncReason.SourceFileNotExist;
				return null;
			}

			if (destinationMetadata != null &&
				destinationMetadata[SynchronizationConstants.RavenSynchronizationConflict] != null
				&& destinationMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] == null)
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
				var rename = localMetadata[SynchronizationConstants.RavenRenameFile];

				if (rename != null)
				{
					if (destinationMetadata != null)
						return new RenameWorkItem(file, rename, localServerUrl, storage);

					return new ContentUpdateWorkItem(rename, localServerUrl, storage, sigGenerator);
					// we have a rename tombstone but file does not exists on destination
				}
				return new DeleteWorkItem(file, localServerUrl, storage);
			}

			if (destinationMetadata != null && Historian.IsDirectChildOfCurrent(localMetadata, destinationMetadata))
			{
				reason = NoSyncReason.ContainedInDestinationHistory;
				return null;
			}

			if (destinationMetadata != null && localMetadata["Content-MD5"] == destinationMetadata["Content-MD5"])
			// file exists on dest and has the same content
			{
				// check metadata to detect if any synchronization is needed
				if (
					localMetadata.AllKeys.Except(new[] { "ETag", "Last-Modified" })
								 .Any(
									 key => !destinationMetadata.AllKeys.Contains(key) || localMetadata[key] != destinationMetadata[key]))
				{
					return new MetadataUpdateWorkItem(file, localServerUrl, destinationMetadata, storage);
				}

				reason = NoSyncReason.SameContentAndMetadata;

				return null; // the same content and metadata - no need to synchronize
			}

			return new ContentUpdateWorkItem(file, localServerUrl, storage, sigGenerator);
		}
	}
}