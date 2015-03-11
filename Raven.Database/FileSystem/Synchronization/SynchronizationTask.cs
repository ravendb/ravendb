using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NLog;
using Raven.Abstractions;
using Raven.Database.Config;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.FileSystem.Connection;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using System.Threading;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Synchronization
{
	public class SynchronizationTask : IDisposable
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		private readonly NotificationPublisher publisher;
		private readonly ITransactionalStorage storage;
		private readonly SynchronizationQueue synchronizationQueue;
		private readonly SynchronizationStrategy synchronizationStrategy;
		private readonly InMemoryRavenConfiguration systemConfiguration;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>> activeIncomingSynchronizations =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>>();

		private readonly IObservable<long> timer;
		private int failedAttemptsToGetDestinationsConfig;
		private IDisposable timerSubscription;

		public SynchronizationTask(ITransactionalStorage storage, SigGenerator sigGenerator, NotificationPublisher publisher,
								   InMemoryRavenConfiguration systemConfiguration)
		{
			this.storage = storage;
			this.publisher = publisher;
			this.systemConfiguration = systemConfiguration;
            this.timer = Observable.Interval(systemConfiguration.FileSystem.MaximumSynchronizationInterval);

			synchronizationQueue = new SynchronizationQueue();
			synchronizationStrategy = new SynchronizationStrategy(storage, sigGenerator);

		    LastSuccessfulSynchronizationTime = DateTime.MinValue;            

			InitializeTimer();
		}

        public DateTime LastSuccessfulSynchronizationTime { get; private set; }

		public string FileSystemUrl
		{
			get { return string.Format("{0}/fs/{1}", systemConfiguration.ServerUrl.TrimEnd('/'), systemConfiguration.FileSystemName); }
		}

		public SynchronizationQueue Queue
		{
			get { return synchronizationQueue; }
		}

        public void IncomingSynchronizationStarted(string fileName, FileSystemInfo sourceFileSystemInfo, Guid sourceFileETag, SynchronizationType type)
        {
            var activeForDestination = activeIncomingSynchronizations.GetOrAdd(sourceFileSystemInfo.Url,
                                                       new ConcurrentDictionary<string, SynchronizationDetails>());

            var syncDetails = new SynchronizationDetails()
            {
                DestinationUrl = sourceFileSystemInfo.Url,
                FileETag = sourceFileETag,
                FileName = fileName,
                Type = type
            };

            if (activeForDestination.TryAdd(fileName, syncDetails))
            {
                Log.Debug("File '{0}' with ETag {1} was added to an incoming active synchronization queue for a destination {2}",
                          fileName,
                          sourceFileETag, sourceFileSystemInfo.Url);
            }
        }

        public void IncomingSynchronizationFinished(string fileName, FileSystemInfo sourceFileSystemInfo, Guid sourceFileETag)
        {
            ConcurrentDictionary<string, SynchronizationDetails> activeSourceTasks;

            if (activeIncomingSynchronizations.TryGetValue(sourceFileSystemInfo.Url, out activeSourceTasks) == false)
            {
                Log.Warn("Could not get an active synchronization queue for {0}", sourceFileSystemInfo.Url);
                return;
            }

            SynchronizationDetails removingItem;
            if (activeSourceTasks.TryRemove(fileName, out removingItem))
            {
                Log.Debug("File '{0}' with ETag {1} was removed from an active synchronization queue for a destination {2}",
                          fileName, sourceFileETag, sourceFileSystemInfo);
            }
        }

        public IEnumerable<SynchronizationDetails> IncomingQueue
        {
            get
            {
                return from destinationActive in activeIncomingSynchronizations
                       from activeFile in destinationActive.Value
                       select activeFile.Value;
            }
        }

		private void InitializeTimer()
		{
			timerSubscription = timer.Subscribe(tick => StartSynchronizeDestinationsInBackground());
		}

		private void StartSynchronizeDestinationsInBackground()
        {
            Task.Factory.StartNew(async () => await Execute(), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        public async Task<DestinationSyncResult> SynchronizeDestinationAsync(string fileSystemDestination, bool forceSyncingContinuation = true)
        {
	        var destination = GetSynchronizationDestinations().FirstOrDefault(x => x.Url.Equals(fileSystemDestination, StringComparison.OrdinalIgnoreCase));

	        if (destination == null)
	        {
				Log.Debug("Could not synchronize to {0} because no destination was configured for that url", fileSystemDestination);
				throw new ArgumentException("Destination files system does not exist", "fileSystemDestination"); 
	        }

	        if (destination.Enabled == false)
		    {
				return new DestinationSyncResult()
		        {
			        Exception = new InvalidOperationException("Configured synchronization destination is disabled")
		        };
			}

			Log.Debug("Starting to synchronize a destination server {0}", destination.Url);

			if (CanSynchronizeTo(destination.Url) == false)
			{
				Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.Url);

				throw new SynchronizationException(string.Format("No synchronization request was available for file system '{0}'", destination.FileSystem));
			}

			return await SynchronizeDestinationAsync(destination, forceSyncingContinuation);
        }

        public async Task<DestinationSyncResult[]> Execute(bool forceSyncingContinuation = true)
		{
			var destinationSyncTasks = new List<Task<DestinationSyncResult>>();

			foreach (var destination in GetSynchronizationDestinations())
			{
                // If the destination is disabled, we skip it.
                if (!destination.Enabled)
                    continue;

				Log.Debug("Starting to synchronize a destination server {0}", destination.Url);

				if (!CanSynchronizeTo(destination.Url))
				{
					Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.Url);
					continue;
				}

				destinationSyncTasks.Add(SynchronizeDestinationAsync(destination, forceSyncingContinuation));
			}

			return await Task.WhenAll(destinationSyncTasks);
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

		    var destinationClient = new AsyncFilesServerClient(destination.ServerUrl, destination.FileSystem, apiKey: destination.ApiKey, credentials: credentials).Synchronization;

            RavenJObject destinationMetadata;

            try
            {
                destinationMetadata = await destinationClient.Commands.GetMetadataForAsync(fileName);
            }
            catch (Exception ex)
            {
                var exceptionMessage = "Could not get metadata details for " + fileName + " from " + destination.Url;
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
				Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destination.Url, reason.GetDescription());

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

                var destinationClient = new AsyncFilesServerClient(destination.ServerUrl, destination.FileSystem,
                                                                  apiKey: destination.ApiKey, credentials: credentials).Synchronization;

				var lastETag = await destinationClient.GetLastSynchronizationFromAsync(storage.Id);

				var activeTasks = synchronizationQueue.Active;
				var filesNeedConfirmation = GetSyncingConfigurations(destination).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

				var confirmations = await ConfirmPushedFiles(filesNeedConfirmation, destinationClient);

				var needSyncingAgain = new List<FileHeader>();

				foreach (var confirmation in confirmations)
				{
					if (confirmation.Status == FileStatus.Safe)
					{
						Log.Debug("Destination server {0} said that file '{1}' is safe", destination, confirmation.FileName);
						RemoveSyncingConfiguration(confirmation.FileName, destination.Url);
					}
					else
					{
						storage.Batch(accessor =>
						{
							var fileHeader = accessor.ReadFile(confirmation.FileName);

							if (fileHeader != null)
							{
								needSyncingAgain.Add(fileHeader);

								Log.Debug("Destination server {0} said that file '{1}' is {2}.", destination, confirmation.FileName, confirmation.Status);
							}
						});
					}
				}

				await EnqueueMissingUpdatesAsync(destinationClient, lastETag, needSyncingAgain);

                var reports = await Task.WhenAll(SynchronizePendingFilesAsync(destination, destinationClient, forceSyncingContinuation));

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
							destination.Url, successfulSynchronizationsCount, failedSynchronizationsCount);
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

        private async Task EnqueueMissingUpdatesAsync(IAsyncFilesSynchronizationCommands destination,
													  SourceSynchronizationInformation lastEtag,
                                                      IList<FileHeader> needSyncingAgain)
		{
			LogFilesInfo("There were {0} file(s) that needed synchronization because the previous one went wrong: {1}",
						 needSyncingAgain);

            var commands = (IAsyncFilesCommandsImpl)destination.Commands;

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
				LogFilesInfo("There were {0} file(s) that needed synchronization after filtering: {1}", filteredFilesToSynchronization);

            // Early break. There are no files to synchronize to the selected destination. 
			if (!filteredFilesToSynchronization.Any())
				return;

            var baseUrl = commands.UrlFor();

			foreach (var fileHeader in filteredFilesToSynchronization)
			{
                var file = fileHeader.FullPath;
				var localMetadata = GetLocalMetadata(file);

				RavenJObject destinationMetadata;

                try
                {
                    destinationMetadata = await destination.Commands.GetMetadataForAsync(file);
                }
                catch (Exception ex)
                {
                    Log.WarnException(
                        string.Format("Could not retrieve a metadata of a file '{0}' from {1} in order to determine needed synchronization type", file,
                            baseUrl), ex);

                    continue;
                }                

				NoSyncReason reason;
                var work = synchronizationStrategy.DetermineWork(file, localMetadata, destinationMetadata, FileSystemUrl, out reason);
				if (work == null)
				{
                    Log.Debug("File '{0}' were not synchronized to {1}. {2}", file, baseUrl, reason.GetDescription());

					if (reason == NoSyncReason.ContainedInDestinationHistory)
					{
                        var etag = localMetadata.Value<Guid>(Constants.MetadataEtagField);
                        await destination.IncrementLastETagAsync(storage.Id, baseUrl, etag);
                        RemoveSyncingConfiguration(file, baseUrl);
					}

					continue;
				}

                if (synchronizationQueue.EnqueueSynchronization(baseUrl, work))
                {
                    publisher.Publish(new SynchronizationUpdateNotification
                    {
                        FileName = work.FileName,
                        DestinationFileSystemUrl = baseUrl,
                        SourceServerId = storage.Id,
                        SourceFileSystemUrl = FileSystemUrl,
                        Type = work.SynchronizationType,
                        Action = SynchronizationAction.Enqueue,
                        Direction = SynchronizationDirection.Outgoing
                    });
                }
			}
		}

        private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(SynchronizationDestination destination, IAsyncFilesSynchronizationCommands destinationCommands, bool forceSyncingContinuation)
		{
            var commands = (IAsyncFilesCommandsImpl)destinationCommands.Commands;

            var destinationUrl = commands.UrlFor();

            for (var i = 0; i < AvailableSynchronizationRequestsTo(destinationUrl); i++)
			{
				SynchronizationWorkItem work;
                if (!synchronizationQueue.TryDequePendingSynchronization(destinationUrl, out work))
					break;

                if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destinationUrl))
				{
					Log.Debug("There was an already being performed synchronization of a file '{0}' to {1}", work.FileName,
							  destinationCommands);

                    if (synchronizationQueue.EnqueueSynchronization(destinationUrl, work)) // add it again at the end of the queue
                    {
                        // add it again at the end of the queue
                        publisher.Publish(new SynchronizationUpdateNotification
                        {
                            FileName = work.FileName,
                            DestinationFileSystemUrl = destinationUrl,
                            SourceServerId = storage.Id,
                            SourceFileSystemUrl = FileSystemUrl,
                            Type = work.SynchronizationType,
                            Action = SynchronizationAction.Enqueue,
                            Direction = SynchronizationDirection.Outgoing
                        });
                    }
				}
				else
				{
					var workTask = PerformSynchronizationAsync(destinationCommands, work);
					if (forceSyncingContinuation)
						workTask.ContinueWith(async t =>
						{
							if (CanSynchronizeTo(destinationUrl))
								await SynchronizeDestinationAsync(destination, true);
						});

                    yield return workTask;
				}
			}
		}

        private async Task<SynchronizationReport> PerformSynchronizationAsync(IAsyncFilesSynchronizationCommands destination,
																			  SynchronizationWorkItem work)
		{
            var commands = (IAsyncFilesCommandsImpl)destination.Commands;
            string destinationUrl = commands.UrlFor();

			Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}",
                       work.GetType().Name, work.FileName, destinationUrl);

            if (!CanSynchronizeTo(destinationUrl))
			{
				Log.Debug("The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
                          destinationUrl, work.FileName);

                if (synchronizationQueue.EnqueueSynchronization(destinationUrl, work))
                {
                    publisher.Publish(new SynchronizationUpdateNotification
                    {
                        FileName = work.FileName,
                        DestinationFileSystemUrl = destinationUrl,
                        SourceServerId = storage.Id,
                        SourceFileSystemUrl = FileSystemUrl,
                        Type = work.SynchronizationType,
                        Action = SynchronizationAction.Enqueue,
                        Direction = SynchronizationDirection.Outgoing
                    });
                }

				return new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = new SynchronizationException(string.Format(
						"The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
                        destinationUrl, work.FileName))
				};
			}

			string fileName = work.FileName;
            synchronizationQueue.SynchronizationStarted(work, destinationUrl);
			publisher.Publish(new SynchronizationUpdateNotification
			{
				FileName = work.FileName,
                DestinationFileSystemUrl = destinationUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Start,
				Direction = SynchronizationDirection.Outgoing
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

                Log.Debug("{0} to {1} has finished successfully{2}", work.ToString(), destinationUrl, moreDetails);
			}
			else
			{
				if (work.IsCancelled || report.Exception is TaskCanceledException)
				{
					synchronizationCancelled = true;
                    Log.DebugException(string.Format("{0} to {1} was canceled", work, destinationUrl), report.Exception);
				}
				else
				{
                    Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destinationUrl), report.Exception);
				}
			}

            Queue.SynchronizationFinished(work, destinationUrl);

			if (!synchronizationCancelled)
                CreateSyncingConfiguration(fileName, work.FileETag, destinationUrl, work.SynchronizationType);

			publisher.Publish(new SynchronizationUpdateNotification
			{
				FileName = work.FileName,
                DestinationFileSystemUrl = destinationUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Finish,
				Direction = SynchronizationDirection.Outgoing
			});

			return report;
		}

        private IEnumerable<FileHeader> GetFilesToSynchronization(SourceSynchronizationInformation destinationsSynchronizationInformationForSource, int take)
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

		private async Task<SynchronizationConfirmation[]> ConfirmPushedFiles(IList<SynchronizationDetails> filesNeedConfirmation, IAsyncFilesSynchronizationCommands commands)
		{
			if (!filesNeedConfirmation.Any())
				return new SynchronizationConfirmation[0];

            return await commands.GetConfirmationForFilesAsync(filesNeedConfirmation.Select(x => new Tuple<string, Etag>(x.FileName, x.FileETag)));
		}

        private IEnumerable<SynchronizationDetails> GetSyncingConfigurations(SynchronizationDestination destination)
		{
			var configObjects = new List<SynchronizationDetails>();

			try
			{
				storage.Batch(
					accessor =>
					{
						configObjects = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNamePrefix + Uri.EscapeUriString(destination.Url), 0, 100)
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
                yield return token.JsonDeserialization<SynchronizationDestination>();
            }
		}

        private bool CanSynchronizeTo(string destinationFileSystemUrl)
		{
			return SynchronizationConfigAccessor.GetOrDefault(storage).MaxNumberOfSynchronizationsPerDestination > synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
		}

        private int AvailableSynchronizationRequestsTo(string destinationFileSystemUrl)
		{
	        if (destinationFileSystemUrl == null)
	        {
		        Debugger.Launch();
		        Debugger.Break();
	        }

			return SynchronizationConfigAccessor.GetOrDefault(storage).MaxNumberOfSynchronizationsPerDestination - synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
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
                      string.Join(",", files.Select(x => string.Format("{0} [ETag {1}]", x.FullPath, x.Metadata.Value<Guid>(Constants.MetadataEtagField)))));
		}

		public void Dispose()
		{
			if (timerSubscription != null)
				timerSubscription.Dispose();
		}
	}
}