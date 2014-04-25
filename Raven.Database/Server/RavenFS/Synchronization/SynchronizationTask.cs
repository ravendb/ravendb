using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NLog;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.RavenFS;
using Raven.Abstractions.Util;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;
using SynchronizationClient = Raven.Client.RavenFS.RavenFileSystemClient.SynchronizationClient;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationTask
	{
		private const int DefaultLimitOfConcurrentSynchronizations = 5;

		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		private readonly NotificationPublisher publisher;
		private readonly ITransactionalStorage storage;
		private readonly SynchronizationQueue synchronizationQueue;
		private readonly SynchronizationStrategy synchronizationStrategy;
		private readonly InMemoryRavenConfiguration systemConfiguration;

		private readonly IObservable<long> timer = Observable.Interval(TimeSpan.FromMinutes(10));
		private int failedAttemptsToGetDestinationsConfig;

		public SynchronizationTask(ITransactionalStorage storage, SigGenerator sigGenerator, NotificationPublisher publisher,
								   InMemoryRavenConfiguration systemConfiguration)
		{
			this.storage = storage;
			this.publisher = publisher;
			this.systemConfiguration = systemConfiguration;
			synchronizationQueue = new SynchronizationQueue();
			synchronizationStrategy = new SynchronizationStrategy(storage, sigGenerator);

		    LastSuccessfulSynchronizationTime = DateTime.MinValue;

			InitializeTimer();
		}

        public DateTime LastSuccessfulSynchronizationTime { get; private set; }

		public string FileSystemUrl
		{
			get { return string.Format("{0}/ravenfs/{1}", systemConfiguration.ServerUrl.TrimEnd('/'), systemConfiguration.FileSystemName); }
		}

		public SynchronizationQueue Queue
		{
			get { return synchronizationQueue; }
		}

		private void InitializeTimer()
		{
			timer.Subscribe(tick => SynchronizeDestinationsAsync());
		}

        public Task<DestinationSyncResult> SynchronizeDestinationAsync(string filesystemDestination, bool forceSyncingContinuation = true)
        {
            foreach (var destination in GetSynchronizationDestinations())
            {
                if (string.Compare(filesystemDestination, destination.FileSystemUrl, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    Log.Debug("Starting to synchronize a destination server {0}", destination.FileSystemUrl);

                    if (!CanSynchronizeTo(destination.FileSystemUrl))
                    {
                        Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.FileSystemUrl);

                        return (Task<DestinationSyncResult>)Task<DestinationSyncResult>.Run(() => 
                            { 
                                throw new SynchronizationException(
                                    string.Format("No synchronization request was available for filesystem '{0}'", destination.FileSystem)); 
                            });
                    }
                    
                    return SynchronizeDestinationAsync(destination, forceSyncingContinuation);
                }
            }

            return (Task<DestinationSyncResult>)Task<DestinationSyncResult>.Run(() => 
            {
                Log.Debug("Could not synchronize to {0} because no destination was configured for that url", filesystemDestination);
                throw new ArgumentException("Filesystem destination does not exist", "filesystemDestination"); 
            });
        }

		public Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingContinuation = true)
		{
			var destinationSyncTasks = new List<Task<DestinationSyncResult>>();

			foreach (var destination in GetSynchronizationDestinations())
			{
				Log.Debug("Starting to synchronize a destination server {0}", destination.FileSystemUrl);

				if (!CanSynchronizeTo(destination.FileSystemUrl))
				{
					Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.FileSystemUrl);
					continue;
				}

				destinationSyncTasks.Add(SynchronizeDestinationAsync(destination, forceSyncingContinuation));
			}

			return Task.WhenAll(destinationSyncTasks);
		}

		public async Task<SynchronizationReport> SynchronizeFileToAsync(string fileName, SynchronizationDestination destination)
		{
            ICredentials credentials = null;
            if (string.IsNullOrEmpty(destination.Username) == false)
            {
                credentials = string.IsNullOrEmpty(destination.Domain)
                                  ? new NetworkCredential(destination.Username, destination.Password)
                                  : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
            }

		    var destinationClient = new RavenFileSystemClient(destination.ServerUrl, destination.FileSystem, apiKey: destination.ApiKey, credentials: credentials).Synchronization;

            RavenJObject destinationMetadata;

            try
            {
                destinationMetadata = await destinationClient.GetMetadataForAsync(fileName);
            }
            catch (Exception ex)
            {
                var exceptionMessage = "Could not get metadata details for " + fileName + " from " + destination.FileSystemUrl;
                Log.WarnException(exceptionMessage, ex);

                return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
                {
                    Exception = new SynchronizationException(exceptionMessage, ex)
                };
            }

            RavenJObject localMetadata = GetLocalMetadata(fileName);

			NoSyncReason reason;
			SynchronizationWorkItem work = synchronizationStrategy.DetermineWork(fileName, localMetadata, destinationMetadata, FileSystemUrl, out reason);

			if (work == null)
			{
				Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destination.FileSystemUrl, reason.GetDescription());

				return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
				{
					Exception = new SynchronizationException(reason.GetDescription())
				};
			}

            return await PerformSynchronizationAsync(destinationClient, work);
		}

        private async Task<DestinationSyncResult> SynchronizeDestinationAsync(SynchronizationDestination destination,
																			  bool forceSyncingContinuation)
		{
			try
			{
                ICredentials credentials = null;
                if (string.IsNullOrEmpty(destination.Username) == false)
                {
                    credentials = string.IsNullOrEmpty(destination.Domain)
                                      ? new NetworkCredential(destination.Username, destination.Password)
                                      : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
                }

                var destinationClient = new RavenFileSystemClient(destination.ServerUrl, destination.FileSystem,
                                                                  apiKey: destination.ApiKey, credentials: credentials).Synchronization;

				var lastETag = await destinationClient.GetLastSynchronizationFromAsync(storage.Id);

				var activeTasks = synchronizationQueue.Active.ToList();
				var filesNeedConfirmation = GetSyncingConfigurations(destination).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

				var confirmations = await ConfirmPushedFiles(filesNeedConfirmation, destinationClient);

				var needSyncingAgain = new List<FileHeader>();

				foreach (var confirmation in confirmations)
				{
					if (confirmation.Status == FileStatus.Safe)
					{
						Log.Debug("Destination server {0} said that file '{1}' is safe", destination, confirmation.FileName);
						RemoveSyncingConfiguration(confirmation.FileName, destination.FileSystemUrl);
					}
					else
					{
						storage.Batch(accessor =>
						{
							var fileHeader = accessor.ReadFile(confirmation.FileName);

							if (fileHeader != null)
							{
								needSyncingAgain.Add(fileHeader);

								Log.Debug("Destination server {0} said that file '{1}' is {2}.", 
                                           destination, confirmation.FileName, confirmation.Status);
							}
						});
					}
				}

				await EnqueueMissingUpdatesAsync(destinationClient, lastETag, needSyncingAgain);

                var reports = await Task.WhenAll(SynchronizePendingFilesAsync(destinationClient, forceSyncingContinuation));

				var destinationSyncResult = new DestinationSyncResult
				{
					DestinationServer = destination.ServerUrl,
                    DestinationFileSystem = destination.FileSystem
				};

				if (reports.Length > 0)
				{
					var successfulSynchronizationsCount = reports.Count(x => x.Exception == null);

					var failedSynchronizationsCount = reports.Count(x => x.Exception != null);

					if (successfulSynchronizationsCount > 0 || failedSynchronizationsCount > 0)
					{
						Log.Debug(
							"Synchronization to a destination {0} has completed. {1} file(s) were synchronized successfully, {2} synchronization(s) were failed",
							destination.FileSystemUrl, successfulSynchronizationsCount, failedSynchronizationsCount);
					}

					destinationSyncResult.Reports = reports;
				}

				return destinationSyncResult;
			}
			catch (Exception ex)
			{
				Log.WarnException(string.Format("Failed to perform a synchronization to a destination {0}", destination), ex);

				return new DestinationSyncResult
				{
					DestinationServer = destination.ServerUrl,
                    DestinationFileSystem = destination.FileSystem,
					Exception = ex
				};
			}
		}

		private async Task EnqueueMissingUpdatesAsync(SynchronizationClient destination,
													  SourceSynchronizationInformation lastEtag,
													  IList<FileHeader> needSyncingAgain)
		{
			LogFilesInfo("There were {0} file(s) that needed synchronization because the previous one went wrong: {1}",
						 needSyncingAgain);

			var filesToSynchronization = new HashSet<FileHeader>(GetFilesToSynchronization(lastEtag, 100),
																 new FileHeaderNameEqualityComparer());

			LogFilesInfo("There were {0} file(s) that needed synchronization because of greater ETag value: {1}",
						 filesToSynchronization);

			foreach (FileHeader needSyncing in needSyncingAgain)
			{
				filesToSynchronization.Add(needSyncing);
			}

			var filteredFilesToSynchronization =
				filesToSynchronization.Where(
					x => synchronizationStrategy.Filter(x, lastEtag.DestinationServerId, filesToSynchronization)).ToList();

			if (filesToSynchronization.Count > 0)
			{
				LogFilesInfo("There were {0} file(s) that needed synchronization after filtering: {1}",
							 filteredFilesToSynchronization);
			}

			if (filteredFilesToSynchronization.Count == 0)
				return;

			foreach (var fileHeader in filteredFilesToSynchronization)
			{
				var file = fileHeader.Name;
				var localMetadata = GetLocalMetadata(file);

				RavenJObject destinationMetadata;

                try
                {
                    destinationMetadata = await destination.GetMetadataForAsync(file);
                }
                catch (Exception ex)
                {
                    Log.WarnException(
                        string.Format("Could not retrieve a metadata of a file '{0}' from {1} in order to determine needed synchronization type", file,
                            destination.FileSystemUrl), ex);

                    continue;
                }

				NoSyncReason reason;
				var work = synchronizationStrategy.DetermineWork(file, localMetadata, destinationMetadata, FileSystemUrl, out reason);

				if (work == null)
				{
					Log.Debug("File '{0}' were not synchronized to {1}. {2}", file, destination.FileSystemUrl, reason.GetDescription());

					if (reason == NoSyncReason.ContainedInDestinationHistory)
					{
						var etag = localMetadata.Value<Guid>("ETag");
						await destination.IncrementLastETagAsync(storage.Id, FileSystemUrl, etag);
						RemoveSyncingConfiguration(file, destination.FileSystemUrl);
					}

					continue;
				}

				synchronizationQueue.EnqueueSynchronization(destination.FileSystemUrl, work);
			}
		}

        private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(SynchronizationClient destination, bool forceSyncingContinuation)
		{
			for (var i = 0; i < AvailableSynchronizationRequestsTo(destination.FileSystemUrl); i++)
			{
				SynchronizationWorkItem work;
				if (!synchronizationQueue.TryDequePendingSynchronization(destination.FileSystemUrl, out work))
					break;

				if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destination.FileSystemUrl))
				{
					Log.Debug("There was an already being performed synchronization of a file '{0}' to {1}", work.FileName,
							  destination);
					synchronizationQueue.EnqueueSynchronization(destination.FileSystemUrl, work); // add it again at the end of the queue
				}
				else
				{
					var workTask = PerformSynchronizationAsync(destination, work);

					if (forceSyncingContinuation)
					{
						workTask.ContinueWith(t => SynchronizePendingFilesAsync(destination, true).ToArray());
					}
					yield return workTask;
				}
			}
		}

		private async Task<SynchronizationReport> PerformSynchronizationAsync(SynchronizationClient destination,
																			  SynchronizationWorkItem work)
		{
			Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}", 
                       work.GetType().Name, work.FileName, destination.FileSystemUrl);

			if (!CanSynchronizeTo(destination.FileSystemUrl))
			{
				Log.Debug("The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						  destination.FileSystemUrl, work.FileName);

				synchronizationQueue.EnqueueSynchronization(destination.FileSystemUrl, work);

				return new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = new SynchronizationException(string.Format(
						"The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						destination.FileSystemUrl, work.FileName))
				};
			}

			string fileName = work.FileName;
			synchronizationQueue.SynchronizationStarted(work, destination.FileSystemUrl);
			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
                DestinationFileSystemUrl = destination.FileSystemUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Start,
				SynchronizationDirection = SynchronizationDirection.Outgoing
			});

			SynchronizationReport report;

			try
			{
				report = await work.PerformAsync(destination);
			}
			catch (Exception ex)
			{
				report = new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = ex,
				};
			}

			var synchronizationCancelled = false;

			if (report.Exception == null)
			{
				var moreDetails = string.Empty;

				if (work.SynchronizationType == SynchronizationType.ContentUpdate)
				{
					moreDetails = string.Format(". {0} bytes were transfered and {1} bytes copied. Need list length was {2}",
												report.BytesTransfered, report.BytesCopied, report.NeedListLength);
				}

                UpdateSuccessfulSynchronizationTime();

				Log.Debug("{0} to {1} has finished successfully{2}", work.ToString(), destination.FileSystemUrl, moreDetails);
			}
			else
			{
				if (work.IsCancelled || report.Exception is TaskCanceledException)
				{
					synchronizationCancelled = true;
					Log.DebugException(string.Format("{0} to {1} was cancelled", work, destination.FileSystemUrl), report.Exception);
				}
				else
				{
					Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destination.FileSystemUrl),
									  report.Exception);
				}
			}

			Queue.SynchronizationFinished(work, destination.FileSystemUrl);

			if (!synchronizationCancelled)
				CreateSyncingConfiguration(fileName, work.FileETag, destination.FileSystemUrl, work.SynchronizationType);

			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
                DestinationFileSystemUrl = destination.FileSystemUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Finish,
				SynchronizationDirection = SynchronizationDirection.Outgoing
			});

			return report;
		}

		private IEnumerable<FileHeader> GetFilesToSynchronization(
			SourceSynchronizationInformation destinationsSynchronizationInformationForSource, int take)
		{
			var filesToSynchronization = new List<FileHeader>();

			Log.Debug("Getting files to synchronize with ETag greater than {0} [parameter take = {1}]",
					  destinationsSynchronizationInformationForSource.LastSourceFileEtag, take);

			try
			{
				storage.Batch(
					accessor =>
					filesToSynchronization =
					accessor.GetFilesAfter(destinationsSynchronizationInformationForSource.LastSourceFileEtag, take).ToList());
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not get files to synchronize after: " +
								  destinationsSynchronizationInformationForSource.LastSourceFileEtag), e);
			}

			return filesToSynchronization;
		}

		private Task<IEnumerable<SynchronizationConfirmation>> ConfirmPushedFiles(
			IList<SynchronizationDetails> filesNeedConfirmation, SynchronizationClient destinationClient)
		{
			if (filesNeedConfirmation.Count == 0)
			{
				return new CompletedTask<IEnumerable<SynchronizationConfirmation>>(Enumerable.Empty<SynchronizationConfirmation>());
			}
			return
				destinationClient.ConfirmFilesAsync(
					filesNeedConfirmation.Select(x => new Tuple<string, Guid>(x.FileName, x.FileETag)));
		}

        private IEnumerable<SynchronizationDetails> GetSyncingConfigurations(SynchronizationDestination destination)
		{
			IList<SynchronizationDetails> configObjects = new List<SynchronizationDetails>();

			try
			{
				storage.Batch(
					accessor =>
					{
						configObjects = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNamePrefix + Uri.EscapeUriString(destination.FileSystemUrl), 0, 100)
									            .Select(config => config.JsonDeserialization<SynchronizationDetails>())
                                                .ToList();
					});
			}
			catch (Exception e)
			{
				Log.WarnException(string.Format("Could not get syncing configurations for a destination {0}", destination), e);
			}

			return configObjects;
		}

        private void CreateSyncingConfiguration(string fileName, Guid etag, string destinationFileSystemUrl, SynchronizationType synchronizationType)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destinationFileSystemUrl);

                var details = new SynchronizationDetails
				{
					DestinationUrl = destinationFileSystemUrl,
					FileName = fileName,
					FileETag = etag,
					Type = synchronizationType
				};

				storage.Batch(accessor => accessor.SetConfig(name, JsonExtensions.ToJObject(details)));
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not create syncing configurations for a file {0} and destination {1}", fileName, destinationFileSystemUrl),
					e);
			}
		}

		private void RemoveSyncingConfiguration(string fileName, string destination)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destination);
				storage.Batch(accessor => accessor.DeleteConfig(name));
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not remove syncing configurations for a file {0} and a destination {1}", fileName, destination),
					e);
			}
		}

        private RavenJObject GetLocalMetadata(string fileName)
		{
            RavenJObject result = null;
            try
            {
                storage.Batch(accessor => { result = accessor.GetFile(fileName, 0, 0).Metadata; });
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            FileAndPages fileAndPages = null;
            {
                try
                {
                    storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
                }
                catch (FileNotFoundException)
                {

                }
            }

            return result;
		}

		private IEnumerable<SynchronizationDestination> GetSynchronizationDestinations()
		{			
            var destinationsConfigExists = false;
			storage.Batch(accessor => destinationsConfigExists = accessor.ConfigExists(SynchronizationConstants.RavenSynchronizationDestinations));
         
			if (!destinationsConfigExists)
			{
				if (failedAttemptsToGetDestinationsConfig < 3 || failedAttemptsToGetDestinationsConfig % 10 == 0)
				{
					Log.Debug("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not exist");
				}

				failedAttemptsToGetDestinationsConfig++;

                yield break;
			}

			failedAttemptsToGetDestinationsConfig = 0;

			var destinationsConfig = new RavenJObject();

			storage.Batch(accessor => destinationsConfig = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationDestinations));

            var destinationsStrings = destinationsConfig.Value<RavenJArray>("Destinations");
            if (destinationsStrings == null)
            {
                Log.Warn("Empty " + SynchronizationConstants.RavenSynchronizationDestinations + " configuration");
                yield break;
            }
            if (destinationsStrings.Count() == 0)
            {
                Log.Warn("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not contain any destination");
                yield break;
            }
            
            foreach ( var token in destinationsStrings )
            {
                yield return JsonExtensions.JsonDeserialization<SynchronizationDestination>((RavenJObject)token);
            }
		}

        private bool CanSynchronizeTo(string destinationFileSystemUrl)
		{
			return LimitOfConcurrentSynchronizations() > synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
		}

        private int AvailableSynchronizationRequestsTo(string destinationFileSystemUrl)
		{
			return LimitOfConcurrentSynchronizations() - synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
		}

		private int LimitOfConcurrentSynchronizations()
		{
			bool limit = false;
			int configuredLimit = 0;

			storage.Batch(
				accessor =>
				limit = accessor.TryGetConfigurationValue(SynchronizationConstants.RavenSynchronizationLimit, out configuredLimit));

			return limit ? configuredLimit : DefaultLimitOfConcurrentSynchronizations;
		}

		public void Cancel(string fileName)
		{
			Log.Debug("Cancellation of active synchronizations of a file '{0}'", fileName);
			Queue.CancelActiveSynchronizations(fileName);
		}

        private void UpdateSuccessfulSynchronizationTime()
        {
            LastSuccessfulSynchronizationTime = SystemTime.UtcNow;
        }

		private static void LogFilesInfo(string message, ICollection<FileHeader> files)
		{
			Log.Debug(message, files.Count,
					  string.Join(",", files.Select(x => string.Format("{0} [ETag {1}]", x.Name, x.Metadata.Value<Guid>("ETag")))));
		}
	}
}