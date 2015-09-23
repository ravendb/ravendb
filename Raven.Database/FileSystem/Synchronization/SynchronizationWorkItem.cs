using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Client.Connection.Profiling;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Synchronization
{
	public abstract class SynchronizationWorkItem : IHoldProfilingInformation
	{
		private readonly ConflictDetector conflictDetector;
		private readonly ConflictResolver conflictResolver;
		protected readonly CancellationTokenSource Cts = new CancellationTokenSource();
        protected FilesConvention Convention = new FilesConvention();
		protected SynchronizationWorkItem(string fileName, string sourceServerUrl, ITransactionalStorage storage)
		{
			Storage = storage;
            FileName = fileName;

			FileAndPagesInformation fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
			FileMetadata = fileAndPages.Metadata;
			FileSystemInfo = new FileSystemInfo
			{
				Id = Storage.Id,
				Url = sourceServerUrl
			};

			conflictDetector = new ConflictDetector();
			conflictResolver = new ConflictResolver(null, null);
		}

		protected ITransactionalStorage Storage { get; private set; }

		public string FileName { get; private set; }

		public Etag FileETag
		{
            get { return Etag.Parse(FileMetadata.Value<string>(Constants.MetadataEtagField)); }
		}

		public bool IsCancelled
		{
			get { return Cts.Token.IsCancellationRequested; }
		}

        protected RavenJObject FileMetadata { get; set; }

		protected FileSystemInfo FileSystemInfo { get; private set; }

		public abstract SynchronizationType SynchronizationType { get; }

		public abstract Task<SynchronizationReport> PerformAsync(ISynchronizationServerClient destination);

		public virtual void Cancel()
		{
		}

        protected void AssertLocalFileExistsAndIsNotConflicted(RavenJObject sourceMetadata)
		{
			if (sourceMetadata == null)
				throw new SynchronizationException(string.Format("File {0} does not exist", FileName));

            if (sourceMetadata.ContainsKey(SynchronizationConstants.RavenSynchronizationConflict))
                throw new SynchronizationException(string.Format("File {0} is conflicted", FileName));
		}

        protected ConflictItem CheckConflictWithDestination(RavenJObject sourceMetadata,
                                                            RavenJObject destinationMetadata, string localServerUrl)
		{
            var conflict = conflictDetector.CheckOnSource(FileName, sourceMetadata, destinationMetadata, localServerUrl);
            var isConflictResolved = conflictResolver.CheckIfResolvedByRemoteStrategy(destinationMetadata, conflict);

            // optimization - conflict checking on source side before any changes pushed
            if (conflict != null && !isConflictResolved)
                return conflict;

            return null;
		}

		private async Task<SynchronizationReport> ApplyConflictOnDestinationAsync(ConflictItem conflict, RavenJObject remoteMetadata, ISynchronizationServerClient synchronizationServerClient, string localServerUrl, ILog log)
		{
			if (log.IsDebugEnabled)
				log.Debug("File '{0}' is in conflict with destination version from {1}. Applying conflict on destination", FileName, synchronizationServerClient.BaseUrl);

			try
			{
				var version = conflict.RemoteHistory.Last().Version;
				var serverId = conflict.RemoteHistory.Last().ServerId;
				var history = new List<HistoryItem>(conflict.RemoteHistory);
				history.RemoveAt(conflict.RemoteHistory.Count - 1);

				await synchronizationServerClient.ApplyConflictAsync(FileName, version, serverId, remoteMetadata, localServerUrl).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				log.WarnException(string.Format("Failed to apply conflict on {0} for file '{1}'", synchronizationServerClient, FileName), ex);
			}

			return new SynchronizationReport(FileName, FileETag, SynchronizationType)
			{
				Exception = new SynchronizationException(string.Format("File {0} is conflicted", FileName)),
			};
		}

		protected async Task<SynchronizationReport> HandleConflict(ISynchronizationServerClient synchronizationServerClient, ConflictItem conflict, ILog log)
		{
			var conflictResolutionStrategy = await synchronizationServerClient.GetResolutionStrategyFromDestinationResolvers(conflict, FileMetadata).ConfigureAwait(false);

			switch (conflictResolutionStrategy)
			{
				case ConflictResolutionStrategy.NoResolution:
					return await ApplyConflictOnDestinationAsync(conflict, FileMetadata, synchronizationServerClient, FileSystemInfo.Url, log).ConfigureAwait(false);
				case ConflictResolutionStrategy.CurrentVersion:
					await ApplyConflictOnDestinationAsync(conflict, FileMetadata, synchronizationServerClient, FileSystemInfo.Url, log).ConfigureAwait(false);
					await synchronizationServerClient.ResolveConflictAsync(FileName, conflictResolutionStrategy).ConfigureAwait(false);
					return new SynchronizationReport(FileName, FileETag, SynchronizationType);
				case ConflictResolutionStrategy.RemoteVersion:
					// we can push the file even though it conflicted, the conflict will be automatically resolved on the destination side
					return null;
				default:
					return new SynchronizationReport(FileName, FileETag, SynchronizationType)
					{
						Exception = new SynchronizationException(string.Format("Unknown resolution strategy: {0}", conflictResolutionStrategy)),
					};
			}
		}

		public void RefreshMetadata()
		{
			if (Storage != null)
			{
				FileAndPagesInformation fileAndPages = null;
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));
				FileMetadata = fileAndPages.Metadata;
			}
		}

		public ProfilingInformation ProfilingInformation { get; private set; }
	}
}
