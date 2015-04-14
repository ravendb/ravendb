using System;
using System.Collections.Specialized;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality
{
	public class ConflictArtifactManager
	{
		private readonly IndexStorage index;
		private readonly ITransactionalStorage storage;

		public ConflictArtifactManager(ITransactionalStorage storage, IndexStorage index)
		{
			this.storage = storage;
			this.index = index;
		}

		public void Create(string fileName, ConflictItem conflict)
		{
            RavenJObject metadata = null;
			MetadataUpdateResult updateMetadata = null;

			storage.Batch(
				accessor =>
				{
					metadata = accessor.GetFile(fileName, 0, 0).Metadata;
					accessor.SetConfig(RavenFileNameHelper.ConflictConfigNameForFile(fileName), JsonExtensions.ToJObject(conflict) );
					metadata[SynchronizationConstants.RavenSynchronizationConflict] = true;
					updateMetadata = accessor.UpdateFileMetadata(fileName, metadata, null);
				});

			if (metadata != null)
				index.Index(fileName, metadata, updateMetadata.Etag);
		}

		public void Delete(string fileName, IStorageActionsAccessor actionsAccessor = null)
		{
            RavenJObject metadata = null;
			MetadataUpdateResult updateResult = null;

			Action<IStorageActionsAccessor> delete = accessor =>
			{
				accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(fileName));
				metadata = accessor.GetFile(fileName, 0, 0).Metadata;
				metadata.Remove(SynchronizationConstants.RavenSynchronizationConflict);
				metadata.Remove(SynchronizationConstants.RavenSynchronizationConflictResolution);
				updateResult = accessor.UpdateFileMetadata(fileName, metadata, null);
			};

			if (actionsAccessor != null)
			{
				delete(actionsAccessor);
			}
			else
			{
				storage.Batch(delete);
			}

			if (metadata != null)
			{
				index.Index(fileName, metadata, updateResult.Etag);
			}
		}
	}
}