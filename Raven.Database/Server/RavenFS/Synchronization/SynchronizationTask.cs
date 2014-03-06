using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NLog;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Util;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationTask
	{
		private const int DefaultLimitOfConcurrentSynchronizations = 5;

		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		private readonly NotificationPublisher publisher;
		private readonly TransactionalStorage storage;
		private readonly SynchronizationQueue synchronizationQueue;
		private readonly SynchronizationStrategy synchronizationStrategy;
		private readonly InMemoryRavenConfiguration systemConfiguration;

		private readonly IObservable<long> timer = Observable.Interval(TimeSpan.FromMinutes(10));
		private int failedAttemptsToGetDestinationsConfig;

		public SynchronizationTask(TransactionalStorage storage, SigGenerator sigGenerator, NotificationPublisher publisher,
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

		public string ServerUrl
		{
			get { return systemConfiguration.ServerUrl; } // TODO arek - we also need to add /ravenfs/FILE_SYSYTEM_NAME_HERE
		}

		public SynchronizationQueue Queue
		{
			get { return synchronizationQueue; }
		}

		private void InitializeTimer()
		{
			timer.Subscribe(tick => SynchronizeDestinationsAsync());
		}

		public Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingContinuation = true)
		{
			var destinationSyncTasks = new List<Task<DestinationSyncResult>>();

			foreach (var destination in GetSynchronizationDestinations())
			{
				Log.Debug("Starting to synchronize a destination server {0}", destination.FileSystemUrl);

				if (!CanSynchronizeTo(destination))
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
			var destinationClient = new RavenFileSystemClient(destination.ServerUrl, destination.FileSystem, apiKey: destination.ApiKey);
			NameValueCollection destinationMetadata;

			try
			{
				destinationMetadata = await destinationClient.GetMetadataForAsync(fileName);
			}
			catch (Exception ex)
			{
				var exceptionMessage = "Could not get metadata details for " + fileName + " from " + destination;
				Log.WarnException(exceptionMessage, ex);

				return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
				{
					Exception = new SynchronizationException(exceptionMessage, ex)
				};
			}

			NameValueCollection localMetadata = GetLocalMetadata(fileName);

			NoSyncReason reason;
			SynchronizationWorkItem work = synchronizationStrategy.DetermineWork(fileName, localMetadata, destinationMetadata,
																				 ServerUrl, out reason);

			if (work == null)
			{
				Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destination, reason.GetDescription());

				return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
				{
					Exception = new SynchronizationException(reason.GetDescription())
				};
			}

			return await PerformSynchronizationAsync(destination, work);
		}

        private async Task<DestinationSyncResult> SynchronizeDestinationAsync(SynchronizationDestination destination,
																			  bool forceSyncingContinuation)
		{
			try
			{
				var destinationClient = new RavenFileSystemClient(destination.ServerUrl, destination.FileSystem, apiKey: destination.ApiKey);

				var lastETag = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(storage.Id);

				var activeTasks = synchronizationQueue.Active.ToList();
				var filesNeedConfirmation =
					GetSyncingConfigurations(destination).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

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

								Log.Debug(
									"Destination server {0} said that file '{1}' is {2}.", destination, confirmation.FileName,
									confirmation.Status);
							}
						});
					}
				}

				await EnqueueMissingUpdatesAsync(destinationClient, destination, lastETag, needSyncingAgain);

				var reports = await Task.WhenAll(SynchronizePendingFilesAsync(destination, forceSyncingContinuation));

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

		private async Task EnqueueMissingUpdatesAsync(RavenFileSystemClient destinationClient,
                                                      SynchronizationDestination destination,
													  SourceSynchronizationInformation lastEtag,
													  IList<FileHeader> needSyncingAgain)
		{
			LogFilesInfo("There were {0} file(s) that needed synchronization because the previous one went wrong: {1}",
						 needSyncingAgain);

			var destinationUrl = destinationClient.ServerUrl;
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

				NameValueCollection destinationMetadata;

				try
				{
					destinationMetadata = await destinationClient.GetMetadataForAsync(file);
				}
				catch (Exception ex)
				{
					Log.WarnException(
						string.Format(
							"Could not retrieve a metadata of a file '{0}' from {1} in order to determine needed synchronization type", file,
							destinationUrl), ex);

					continue;
				}

				NoSyncReason reason;
				var work = synchronizationStrategy.DetermineWork(file, localMetadata, destinationMetadata, ServerUrl, out reason);

				if (work == null)
				{
					Log.Debug("File '{0}' were not synchronized to {1}. {2}", file, destinationUrl, reason.GetDescription());

					if (reason == NoSyncReason.ContainedInDestinationHistory)
					{
						var etag = localMetadata.Value<Guid>("ETag");
						destinationClient.Synchronization.IncrementLastETagAsync(storage.Id, ServerUrl, etag);
						RemoveSyncingConfiguration(file, destinationClient.ServerUrl);
					}

					continue;
				}

				synchronizationQueue.EnqueueSynchronization(destination, work);
			}
		}

        private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(SynchronizationDestination destination, bool forceSyncingContinuation)
		{
			for (var i = 0; i < AvailableSynchronizationRequestsTo(destination); i++)
			{
				SynchronizationWorkItem work;
				if (!synchronizationQueue.TryDequePendingSynchronization(destination, out work))
					break;

				if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destination))
				{
					Log.Debug("There was an already being performed synchronization of a file '{0}' to {1}", work.FileName,
							  destination);
					synchronizationQueue.EnqueueSynchronization(destination, work); // add it again at the end of the queue
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

		private async Task<SynchronizationReport> PerformSynchronizationAsync(SynchronizationDestination destination,
																			  SynchronizationWorkItem work)
		{
			Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}", work.GetType().Name, work.FileName,
					  destination.FileSystemUrl);

			if (!CanSynchronizeTo(destination))
			{
				Log.Debug("The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						  destination.FileSystemUrl, work.FileName);

				synchronizationQueue.EnqueueSynchronization(destination, work);

				return new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = new SynchronizationException(string.Format(
						"The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						destination.FileSystemUrl, work.FileName))
				};
			}

			string fileName = work.FileName;
			synchronizationQueue.SynchronizationStarted(work, destination);
			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
				DestinationServer = destination.ServerUrl,
                DestinationFileSystem = destination.FileSystem,
				SourceServerId = storage.Id,
				SourceServerUrl = ServerUrl,
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
					Log.DebugException(string.Format("{0} to {1} was cancelled", work, destination), report.Exception);
				}
				else
				{
					Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destination),
									  report.Exception);
				}
			}

			Queue.SynchronizationFinished(work, destination);

			if (!synchronizationCancelled)
				CreateSyncingConfiguration(fileName, work.FileETag, destination, work.SynchronizationType);

			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
				DestinationServer = destination.ServerUrl,
                DestinationFileSystem = destination.FileSystem,
				SourceServerId = storage.Id,
				SourceServerUrl = ServerUrl,
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
			IList<SynchronizationDetails> filesNeedConfirmation, RavenFileSystemClient destinationClient)
		{
			if (filesNeedConfirmation.Count == 0)
			{
				return new CompletedTask<IEnumerable<SynchronizationConfirmation>>(Enumerable.Empty<SynchronizationConfirmation>());
			}
			return
				destinationClient.Synchronization.ConfirmFilesAsync(
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
						configObjects =
							accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNamePrefix + Uri.EscapeUriString(destination.FileSystemUrl), 0, 100)
									.Select(config => config.AsObject<SynchronizationDetails>()).ToList();
					});
			}
			catch (Exception e)
			{
				Log.WarnException(string.Format("Could not get syncing configurations for a destination {0}", destination), e);
			}

			return configObjects;
		}

        private void CreateSyncingConfiguration(string fileName, Guid etag, SynchronizationDestination destination,
												SynchronizationType synchronizationType)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destination.FileSystemUrl);
				storage.Batch(accessor => accessor.SetConfig(name, new SynchronizationDetails
				{
					DestinationUrl = destination.FileSystemUrl,
					FileName = fileName,
					FileETag = etag,
					Type = synchronizationType
				}.AsConfig()));
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not create syncing configurations for a file {0} and destination {1}", fileName, destination),
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

		private NameValueCollection GetLocalMetadata(string fileName)
		{
			NameValueCollection result = null;
			try
			{
				storage.Batch(
					accessor => { result = accessor.GetFile(fileName, 0, 0).Metadata; });
			}
			catch (FileNotFoundException)
			{
				return null;
			}
			FileAndPages fileAndPages = null;
			{
				try
				{
					storage.Batch(accessor => fileAndPages  = accessor.GetFile(fileName, 0, 0));
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
			storage.Batch(
				accessor =>
				destinationsConfigExists = accessor.ConfigExists(SynchronizationConstants.RavenSynchronizationDestinations));

			if (!destinationsConfigExists)
			{
				if (failedAttemptsToGetDestinationsConfig < 3 || failedAttemptsToGetDestinationsConfig % 10 == 0)
				{
					Log.Debug("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not exist");
				}

				failedAttemptsToGetDestinationsConfig++;

				return Enumerable.Empty<SynchronizationDestination>();
			}

			failedAttemptsToGetDestinationsConfig = 0;

			var destinationsConfig = new NameValueCollection();

			storage.Batch(
				accessor => destinationsConfig = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationDestinations));

		    var destinationsStrings = destinationsConfig.GetValues("destination");

			if (destinationsStrings == null)
			{
				Log.Warn("Empty " + SynchronizationConstants.RavenSynchronizationDestinations + " configuration");
				return Enumerable.Empty<SynchronizationDestination>();
			}

            var destinations = destinationsStrings.Select(JsonConvert.DeserializeObject<SynchronizationDestination>).ToArray();

            if (destinations.Length == 0)
			{
				Log.Warn("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations +
						 " does not contain any destination");
			}

            return destinations;
		}

        private bool CanSynchronizeTo(SynchronizationDestination destination)
		{
			return LimitOfConcurrentSynchronizations() > synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destination);
		}

        private int AvailableSynchronizationRequestsTo(SynchronizationDestination destination)
		{
			return LimitOfConcurrentSynchronizations() - synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destination);
		}

		private int LimitOfConcurrentSynchronizations()
		{
			var limit = false;
			var configuredLimit = 0;

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