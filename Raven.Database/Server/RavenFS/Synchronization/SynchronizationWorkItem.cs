using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.RavenFS;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Connections;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Conflictuality;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public abstract class SynchronizationWorkItem : IHoldProfilingInformation
	{
		private readonly ConflictDetector conflictDetector;
		private readonly ConflictResolver conflictResolver;
		protected readonly CancellationTokenSource Cts = new CancellationTokenSource();
		protected FileConvention Convention = new FileConvention();
		protected SynchronizationWorkItem(string fileName, string sourceServerUrl, ITransactionalStorage storage)
		{
			Storage = storage;
			FileName = fileName;

			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
			FileMetadata = fileAndPages.Metadata;
			ServerInfo = new ServerInfo
			{
				Id = Storage.Id,
				FileSystemUrl = sourceServerUrl
			};

			conflictDetector = new ConflictDetector();
			conflictResolver = new ConflictResolver();
		}

		protected ITransactionalStorage Storage { get; private set; }

		public string FileName { get; private set; }

		public Guid FileETag
		{
			get { return FileMetadata.Value<Guid>("ETag"); }
		}

		public bool IsCancelled
		{
			get { return Cts.Token.IsCancellationRequested; }
		}

		protected NameValueCollection FileMetadata { get; set; }

		protected ServerInfo ServerInfo { get; private set; }

		public abstract SynchronizationType SynchronizationType { get; }

        public abstract Task<SynchronizationReport> PerformAsync(RavenFileSystemClient.SynchronizationClient destination);

		public virtual void Cancel()
		{
		}

		protected void AssertLocalFileExistsAndIsNotConflicted(NameValueCollection sourceMetadata)
		{
			if (sourceMetadata == null)
				throw new SynchronizationException(string.Format("File {0} does not exist", FileName));

			if (sourceMetadata.AllKeys.Contains(SynchronizationConstants.RavenSynchronizationConflict))
				throw new SynchronizationException(string.Format("File {0} is conflicted", FileName));
		}

		protected ConflictItem CheckConflictWithDestination(NameValueCollection sourceMetadata,
															NameValueCollection destinationMetadata, string localServerUrl)
		{
			var conflict = conflictDetector.CheckOnSource(FileName, sourceMetadata, destinationMetadata, localServerUrl);
			var isConflictResolved = conflictResolver.IsResolved(destinationMetadata, conflict);

			// optimization - conflict checking on source side before any changes pushed
			if (conflict != null && !isConflictResolved)
				return conflict;

			return null;
		}

        protected async Task<SynchronizationReport> ApplyConflictOnDestinationAsync(ConflictItem conflict, RavenFileSystemClient.SynchronizationClient destination,
																					string localServerUrl, ILog log)
		{
			log.Debug("File '{0}' is in conflict with destination version from {1}. Applying conflict on destination", FileName,
					  destination.FileSystemUrl);

			try
			{
				var version = conflict.RemoteHistory.Last().Version;
				var serverId = conflict.RemoteHistory.Last().ServerId;
				var history = new List<HistoryItem>(conflict.RemoteHistory);
				history.RemoveAt(conflict.RemoteHistory.Count - 1);

				await destination.ApplyConflictAsync(FileName, version, serverId, history, localServerUrl);
			}
			catch (Exception ex)
			{
				log.WarnException(string.Format("Failed to apply conflict on {0} for file '{1}'", destination, FileName), ex);
			}

			return new SynchronizationReport(FileName, FileETag, SynchronizationType)
			{
				Exception = new SynchronizationException(string.Format("File {0} is conflicted", FileName)),
			};
		}

		public void RefreshMetadata()
		{
			if (Storage != null)
			{
				FileAndPages fileAndPages = null;
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));
				FileMetadata = fileAndPages.Metadata;
			}
		}

		public ProfilingInformation ProfilingInformation { get; private set; }
	}
}
