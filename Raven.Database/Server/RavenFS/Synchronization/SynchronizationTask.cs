using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NLog;
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

			InitializeTimer();
		}

		public string ServerUrl
		{
			get { return systemConfiguration.ServerUrl; }
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
				Log.Debug("Starting to synchronize a destination server {0}", destination);

				var destinationUrl = destination;

				if (!CanSynchronizeTo(destinationUrl))
				{
					Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination);
					continue;
				}

				destinationSyncTasks.Add(SynchronizeDestinationAsync(destinationUrl, forceSyncingContinuation));
			}

			return Task.WhenAll(destinationSyncTasks);
		}

		public async Task<SynchronizationReport> SynchronizeFileToAsync(string fileName, string destinationUrl)
		{
			var destinationClient = new RavenFileSystemClient(destinationUrl);
			NameValueCollection destinationMetadata;

			try
			{
				destinationMetadata = await destinationClient.GetMetadataForAsync(fileName);
			}
			catch (Exception ex)
			{
				var exceptionMessage = "Could not get metadata details for " + fileName + " from " + destinationUrl;
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
				Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destinationUrl, reason.GetDescription());

				return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
				{
					Exception = new SynchronizationException(reason.GetDescription())
				};
			}

			return await PerformSynchronizationAsync(destinationClient.ServerUrl, work);
		}

		private async Task<DestinationSyncResult> SynchronizeDestinationAsync(string destinationUrl,
																			  bool forceSyncingContinuation)
		{
			try
			{
				var destinationClient = new RavenFileSystemClient(destinationUrl);

				var lastETag = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(storage.Id);

				var activeTasks = synchronizationQueue.Active.ToList();
				var filesNeedConfirmation =
					GetSyncingConfigurations(destinationUrl).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

				var confirmations = await ConfirmPushedFiles(filesNeedConfirmation, destinationClient);

				var needSyncingAgain = new List<FileHeader>();

				foreach (var confirmation in confirmations)
				{
					if (confirmation.Status == FileStatus.Safe)
					{
						Log.Debug("Destination server {0} said that file '{1}' is safe", destinationUrl, confirmation.FileName);
						RemoveSyncingConfiguration(confirmation.FileName, destinationUrl);
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
									"Destination server {0} said that file '{1}' is {2}.", destinationUrl, confirmation.FileName,
									confirmation.Status);
							}
						});
					}
				}

				await EnqueueMissingUpdatesAsync(destinationClient, lastETag, needSyncingAgain);

				var reports = await Task.WhenAll(SynchronizePendingFilesAsync(destinationUrl, forceSyncingContinuation));

				var destinationSyncResult = new DestinationSyncResult
				{
					DestinationServer = destinationUrl
				};

				if (reports.Length > 0)
				{
					var successfullSynchronizationsCount = reports.Count(x => x.Exception == null);

					var failedSynchronizationsCount = reports.Count(x => x.Exception != null);

					if (successfullSynchronizationsCount > 0 || failedSynchronizationsCount > 0)
					{
						Log.Debug(
							"Synchronization to a destination {0} has completed. {1} file(s) were synchronized successfully, {2} synchronization(s) were failed",
							destinationUrl, successfullSynchronizationsCount, failedSynchronizationsCount);
					}

					destinationSyncResult.Reports = reports;
				}

				return destinationSyncResult;
			}
			catch (Exception ex)
			{
				Log.WarnException(string.Format("Failed to perform a synchronization to a destination {0}", destinationUrl), ex);

				return new DestinationSyncResult
				{
					DestinationServer = destinationUrl,
					Exception = ex
				};
			}
		}

		private async Task EnqueueMissingUpdatesAsync(RavenFileSystemClient destinationClient,
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

				synchronizationQueue.EnqueueSynchronization(destinationUrl, work);
			}
		}

		private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(string destinationUrl, bool forceSyncingContinuation)
		{
			for (var i = 0; i < AvailableSynchronizationRequestsTo(destinationUrl); i++)
			{
				SynchronizationWorkItem work;
				if (!synchronizationQueue.TryDequePendingSynchronization(destinationUrl, out work))
					break;

				if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destinationUrl))
				{
					Log.Debug("There was an already being performed synchronization of a file '{0}' to {1}", work.FileName,
							  destinationUrl);
					synchronizationQueue.EnqueueSynchronization(destinationUrl, work); // add it again at the end of the queue
				}
				else
				{
					var workTask = PerformSynchronizationAsync(destinationUrl, work);

					if (forceSyncingContinuation)
					{
						workTask.ContinueWith(t => SynchronizePendingFilesAsync(destinationUrl, true).ToArray());
					}
					yield return workTask;
				}
			}
		}

		private async Task<SynchronizationReport> PerformSynchronizationAsync(string destinationUrl,
																			  SynchronizationWorkItem work)
		{
			Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}", work.GetType().Name, work.FileName,
					  destinationUrl);

			if (!CanSynchronizeTo(destinationUrl))
			{
				Log.Debug("The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						  destinationUrl, work.FileName);

				synchronizationQueue.EnqueueSynchronization(destinationUrl, work);

				return new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = new SynchronizationException(string.Format(
						"The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
						destinationUrl, work.FileName))
				};
			}

			string fileName = work.FileName;
			synchronizationQueue.SynchronizationStarted(work, destinationUrl);
			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
				DestinationServer = destinationUrl,
				SourceServerId = storage.Id,
				SourceServerUrl = ServerUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Start,
				SynchronizationDirection = SynchronizationDirection.Outgoing
			});

			SynchronizationReport report;

			try
			{
				report = await work.PerformAsync(destinationUrl);
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

				Log.Debug("{0} to {1} has finished successfully{2}", work.ToString(), destinationUrl, moreDetails);
			}
			else
			{
				if (work.IsCancelled || report.Exception is TaskCanceledException)
				{
					synchronizationCancelled = true;
					Log.DebugException(string.Format("{0} to {1} was cancelled", work, destinationUrl), report.Exception);
				}
				else
				{
					Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destinationUrl),
									  report.Exception);
				}
			}

			Queue.SynchronizationFinished(work, destinationUrl);

			if (!synchronizationCancelled)
				CreateSyncingConfiguration(fileName, work.FileETag, destinationUrl, work.SynchronizationType);

			publisher.Publish(new SynchronizationUpdate
			{
				FileName = work.FileName,
				DestinationServer = destinationUrl,
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

		private IEnumerable<SynchronizationDetails> GetSyncingConfigurations(string destination)
		{
			IList<SynchronizationDetails> configObjects = new List<SynchronizationDetails>();

			try
			{
				storage.Batch(
					accessor =>
					{
						configObjects =
							accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNamePrefix + Uri.EscapeUriString(destination), 0, 100)
									.Select(config => config.AsObject<SynchronizationDetails>()).ToList();
					});
			}
			catch (Exception e)
			{
				Log.WarnException(string.Format("Could not get syncing configurations for a destination {0}", destination), e);
			}

			return configObjects;
		}

		private void CreateSyncingConfiguration(string fileName, Guid etag, string destination,
												SynchronizationType synchronizationType)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destination);
				storage.Batch(accessor => accessor.SetConfig(name, new SynchronizationDetails
				{
					DestinationUrl = destination,
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
			return result;
		}

		private IEnumerable<string> GetSynchronizationDestinations()
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

				return Enumerable.Empty<string>();
			}

			failedAttemptsToGetDestinationsConfig = 0;

			var destinationsConfig = new NameValueCollection();

			storage.Batch(
				accessor => destinationsConfig = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationDestinations));

			var destinations = destinationsConfig.GetValues("url");

			if (destinations == null)
			{
				Log.Warn("Empty " + SynchronizationConstants.RavenSynchronizationDestinations + " configuration");
				return Enumerable.Empty<string>();
			}

			for (var i = 0; i < destinations.Length; i++)
			{
				if (destinations[i].EndsWith("/"))
					destinations[i] = destinations[i].Substring(0, destinations[i].Length - 1);
			}

			if (destinations.Length == 0)
			{
				Log.Warn("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations +
						 " does not contain any destination");
			}

			return destinations;
		}

		private bool CanSynchronizeTo(string destination)
		{
			return LimitOfConcurrentSynchronizations() > synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destination);
		}

		private int AvailableSynchronizationRequestsTo(string destination)
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

		private static void LogFilesInfo(string message, ICollection<FileHeader> files)
		{
			Log.Debug(message, files.Count,
					  string.Join(",", files.Select(x => string.Format("{0} [ETag {1}]", x.Name, x.Metadata.Value<Guid>("ETag")))));
		}
	}
}