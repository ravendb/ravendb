using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Config;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationTask : IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        internal const int NumberOfFilesToCheckForSynchronization = 100;

        private readonly NotificationPublisher publisher;
        private readonly ITransactionalStorage storage;
        private readonly SynchronizationQueue synchronizationQueue;
        private readonly SynchronizationStrategy synchronizationStrategy;
        private readonly InMemoryRavenConfiguration systemConfiguration;
        private readonly SynchronizationTaskContext context;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>> activeIncomingSynchronizations =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>>();

        private int failedAttemptsToGetDestinationsConfig;
        private long workCounter;

        private Task task;

        public SynchronizationTask(ITransactionalStorage storage, SigGenerator sigGenerator, NotificationPublisher publisher, InMemoryRavenConfiguration systemConfiguration)
        {
            this.storage = storage;
            this.publisher = publisher;
            this.systemConfiguration = systemConfiguration;

            context = new SynchronizationTaskContext();
            synchronizationQueue = new SynchronizationQueue();
            synchronizationStrategy = new SynchronizationStrategy(storage, sigGenerator, systemConfiguration);
        }

        public string FileSystemUrl
        {
            get { return string.Format("{0}/fs/{1}", systemConfiguration.ServerUrl.TrimEnd('/'), systemConfiguration.FileSystemName); }
        }

        public SynchronizationQueue Queue
        {
            get { return synchronizationQueue; }
        }

        public SynchronizationTaskContext Context
        {
            get { return context; }
        }

        public void IncomingSynchronizationStarted(string fileName, FileSystemInfo sourceFileSystemInfo, Guid sourceFileETag, SynchronizationType type)
        {
            var activeForDestination = activeIncomingSynchronizations.GetOrAdd(sourceFileSystemInfo.Url,
                                                       new ConcurrentDictionary<string, SynchronizationDetails>());

            var syncDetails = new SynchronizationDetails
            {
                DestinationUrl = sourceFileSystemInfo.Url,
                FileETag = sourceFileETag,
                FileName = fileName,
                Type = type
            };

            if (activeForDestination.TryAdd(fileName, syncDetails))
            {
                if (Log.IsDebugEnabled)
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
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' with ETag {1} was removed from an active incoming synchronizations for a source {2}",
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

        public async Task<DestinationSyncResult> SynchronizeDestinationAsync(string fileSystemDestination, bool forceSyncingAll)
        {
            var destination = GetSynchronizationDestinations().FirstOrDefault(x => x.Url.Equals(fileSystemDestination, StringComparison.OrdinalIgnoreCase));

            if (destination == null)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Could not synchronize to {0} because no destination was configured for that url", fileSystemDestination);
                throw new ArgumentException("Destination files system does not exist", "fileSystemDestination");
            }

            if (destination.Enabled == false)
            {
                return new DestinationSyncResult
                {
                    Exception = new InvalidOperationException("Configured synchronization destination is disabled")
                };
            }

            if (Log.IsDebugEnabled)
                Log.Debug("Starting to synchronize a destination server {0}", destination.Url);

            if (AvailableSynchronizationRequestsTo(destination.Url) <= 0)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.Url);

                throw new SynchronizationException(string.Format("No synchronization request was available for file system '{0}'", destination.FileSystem));
            }

            return await CreateDestinationResult(destination, await SynchronizeDestinationAsync(destination, forceSyncingAll).ConfigureAwait(false)).ConfigureAwait(false);
        }

        public async Task<DestinationSyncResult> CreateDestinationResult(SynchronizationDestination destination, IEnumerable<Task<SynchronizationReport>> synchronizations)
        {
            try
            {
                var reports = await Task.WhenAll(synchronizations).ConfigureAwait(false);

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
                        if (Log.IsDebugEnabled)
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

        public void Start()
        {
            task = new Task(() =>
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Starting the synchronization task");

                var timeToWait = systemConfiguration.FileSystem.MaximumSynchronizationInterval;

                while (context.DoWork)
                {
                    try
                    {
                        var result = Execute(true);
                        var synchronizations = result.Values.SelectMany(x => x.Result as IEnumerable<Task>);

                        Task.WaitAny(synchronizations.ToArray()); // if any synchronization slot gets released try to schedule another synchronization 
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException("Failed to perform synchronization", e.SimplifyException());
                    }

                    var runningBecauseOfDataModifications = context.WaitForWork(timeToWait, ref workCounter);

                    timeToWait = runningBecauseOfDataModifications
                        ? TimeSpan.FromSeconds(30)
                        : systemConfiguration.FileSystem.MaximumSynchronizationInterval;
                }
            }, TaskCreationOptions.LongRunning);

            task.Start();
        }

        public Dictionary<SynchronizationDestination, Task<IEnumerable<Task<SynchronizationReport>>>> Execute(bool forceSyncingAll)
        {
            var destinationSyncs = new Dictionary<SynchronizationDestination, Task<IEnumerable<Task<SynchronizationReport>>>>();

            foreach (var dst in GetSynchronizationDestinations())
            {
                SynchronizationDestination destination = dst;

                // If the destination is disabled, we skip it.
                if (destination.Enabled == false)
                    continue;

                if (Log.IsDebugEnabled)
                    Log.Debug("Starting to synchronize a destination server {0}", dst.Url);

                if (AvailableSynchronizationRequestsTo(destination.Url) <= 0)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("Could not synchronize to {0} because no synchronization request was available", dst.Url);
                    continue;
                }

                destinationSyncs.Add(destination, SynchronizeDestinationAsync(destination, forceSyncingAll));
            }

            return destinationSyncs;
        }

        public async Task<SynchronizationReport> SynchronizeFileToAsync(string fileName, SynchronizationDestination destination)
        {
            ICredentials credentials = destination.Credentials;

            var conventions = new FilesConvention();
            if (string.IsNullOrEmpty(destination.AuthenticationScheme) == false)
                conventions.AuthenticationScheme = destination.AuthenticationScheme;

            var destinationClient = new SynchronizationServerClient(destination.ServerUrl, destination.FileSystem, convention: conventions, apiKey: destination.ApiKey, credentials: credentials);

            RavenJObject destinationMetadata;

            try
            {
                destinationMetadata = await destinationClient.GetMetadataForAsync(fileName).ConfigureAwait(false);
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
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destination.Url, reason.GetDescription());

                return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
                {
                    Exception = new SynchronizationException(reason.GetDescription())
                };
            }

            return await PerformSynchronizationAsync(destinationClient, work).ConfigureAwait(false);
        }

        private async Task<IEnumerable<Task<SynchronizationReport>>> SynchronizeDestinationAsync(SynchronizationDestination destination, bool forceSyncingAll)
        {
            ICredentials credentials = destination.Credentials;

            var destinationSyncClient = new SynchronizationServerClient(destination.ServerUrl, destination.FileSystem, destination.ApiKey, credentials);

            bool repeat;

            do
            {
                var lastETag = await destinationSyncClient.GetLastSynchronizationFromAsync(storage.Id).ConfigureAwait(false);

                var activeTasks = synchronizationQueue.Active;
                var filesNeedConfirmation = GetSyncingConfigurations(destination).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

                var confirmations = await ConfirmPushedFiles(filesNeedConfirmation, destinationSyncClient).ConfigureAwait(false);

                var needSyncingAgain = new List<FileHeader>();

                foreach (var confirmation in confirmations)
                {
                    if (confirmation.Status == FileStatus.Safe)
                    {
                        if (Log.IsDebugEnabled)
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

                                if (Log.IsDebugEnabled)
                                    Log.Debug("Destination server {0} said that file '{1}' is {2}.", destination, confirmation.FileName, confirmation.Status);
                            }
                        });
                    }
                }

                if (synchronizationQueue.NumberOfPendingSynchronizationsFor(destination.Url) < AvailableSynchronizationRequestsTo(destination.Url))
                {
                    repeat = await EnqueueMissingUpdatesAsync(destinationSyncClient, lastETag, needSyncingAgain).ConfigureAwait(false) == false;
                }
                else
                    repeat = false;
            }
            while (repeat);

            return SynchronizePendingFilesAsync(destinationSyncClient, forceSyncingAll);
        }

        private async Task<bool> EnqueueMissingUpdatesAsync(ISynchronizationServerClient destinationSyncClient,
                                                      SourceSynchronizationInformation synchronizationInfo,
                                                      IList<FileHeader> needSyncingAgain)
        {
            LogFilesInfo("There were {0} file(s) that needed synchronization because the previous one went wrong: {1}",
                         needSyncingAgain);

            var filesToSynchronization = new HashSet<FileHeader>(GetFilesToSynchronization(synchronizationInfo.LastSourceFileEtag, NumberOfFilesToCheckForSynchronization),
                                                                    FileHeaderNameEqualityComparer.Instance);

            LogFilesInfo("There were {0} file(s) that needed synchronization because of greater ETag value: {1}",
                            filesToSynchronization);

            foreach (FileHeader needSyncing in needSyncingAgain)
            {
                filesToSynchronization.Add(needSyncing);
            }

            var filteredFilesToSynchronization = filesToSynchronization.Where(
                x => synchronizationStrategy.Filter(x, synchronizationInfo.DestinationServerId, filesToSynchronization)).ToList();

            if (filesToSynchronization.Count > 0)
                LogFilesInfo("There were {0} file(s) that needed synchronization after filtering: {1}", filteredFilesToSynchronization);

            if (filteredFilesToSynchronization.Count == 0)
            {
                var lastFileBeforeFiltering = filesToSynchronization.LastOrDefault();

                if (lastFileBeforeFiltering == null)
                    return true; // there are no more files that need

                if (lastFileBeforeFiltering.Etag == synchronizationInfo.LastSourceFileEtag)
                    return true; // already updated etag on destination side

                await destinationSyncClient.IncrementLastETagAsync(storage.Id, FileSystemUrl, lastFileBeforeFiltering.Etag).ConfigureAwait(false);
                return false; // all docs has been filtered out, update etag on destination side and retry
            }

            var destinationUrl = destinationSyncClient.BaseUrl;

            bool enqueued = false;
            Etag incrementedEtag = Etag.Empty;

            foreach (var fileHeader in filteredFilesToSynchronization)
            {
                context.CancellationToken.ThrowIfCancellationRequested();

                var file = fileHeader.FullPath;
                var localMetadata = GetLocalMetadata(file);

                RavenJObject destinationMetadata;

                try
                {
                    destinationMetadata = await destinationSyncClient.GetMetadataForAsync(file).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Log.WarnException(
                        string.Format("Could not retrieve a metadata of a file '{0}' from {1} in order to determine needed synchronization type", file,
                            destinationUrl), ex);

                    continue;
                }

                NoSyncReason reason;
                var work = synchronizationStrategy.DetermineWork(file, localMetadata, destinationMetadata, FileSystemUrl, out reason);

                if (work == null)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("File '{0}' were not synchronized to {1}. {2}", file, destinationUrl, reason.GetDescription());

                    if (reason == NoSyncReason.ContainedInDestinationHistory)
                    {
                        var etag = Etag.Parse(localMetadata.Value<string>(Constants.MetadataEtagField));

                        await destinationSyncClient.IncrementLastETagAsync(storage.Id, FileSystemUrl, etag).ConfigureAwait(false);
                        RemoveSyncingConfiguration(file, destinationUrl);

                        if (EtagUtil.IsGreaterThan(etag, incrementedEtag))
                            incrementedEtag = etag;
                    }
                    else if (reason == NoSyncReason.DestinationFileConflicted)
                    {
                        if (needSyncingAgain.Contains(fileHeader, FileHeaderNameEqualityComparer.Instance) == false)
                            CreateSyncingConfiguration(fileHeader.Name, fileHeader.Etag, destinationUrl, SynchronizationType.Unknown);

                        var etag = Etag.Parse(localMetadata.Value<string>(Constants.MetadataEtagField));
                        await destinationSyncClient.IncrementLastETagAsync(storage.Id, FileSystemUrl, etag).ConfigureAwait(false);

                        if (EtagUtil.IsGreaterThan(etag, incrementedEtag))
                            incrementedEtag = etag;
                    }

                    continue;
                }

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

                enqueued = true;
            }

            if (enqueued == false && EtagUtil.IsGreaterThan(incrementedEtag, synchronizationInfo.LastSourceFileEtag))
                return false; // we bumped the last synced etag on a destination server, let it know it need to repeat the operation

            return true;
        }

        private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(ISynchronizationServerClient destinationCommands, bool forceSyncingAll)
        {
            var destinationUrl = destinationCommands.BaseUrl;

            while (AvailableSynchronizationRequestsTo(destinationUrl) > 0)
            {
                context.CancellationToken.ThrowIfCancellationRequested();

                SynchronizationWorkItem work;
                if (synchronizationQueue.TryDequePending(destinationUrl, out work) == false)
                    break;

                if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destinationUrl))
                {
                    if (Log.IsDebugEnabled)
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

                    continue;
                }

                var workTask = PerformSynchronizationAsync(destinationCommands, work);

                if (forceSyncingAll)
                {
                    workTask.ContinueWith(_ => context.NotifyAboutWork()); // synchronization slot released, next file can be synchronized
                }

                yield return workTask;
            }
        }

        private async Task<SynchronizationReport> PerformSynchronizationAsync(ISynchronizationServerClient synchronizationServerClient,
                                                                              SynchronizationWorkItem work)
        {
            var destinationUrl = synchronizationServerClient.BaseUrl;

            if (Log.IsDebugEnabled)
                Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}",
                       work.GetType().Name, work.FileName, destinationUrl);

            if (AvailableSynchronizationRequestsTo(destinationUrl) <= 0)
            {
                if (Log.IsDebugEnabled)
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
                report = await work.PerformAsync(synchronizationServerClient).ConfigureAwait(false);
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
                    moreDetails = string.Format(". {0} bytes were transferred and {1} bytes copied. Need list length was {2}",
                                                report.BytesTransfered, report.BytesCopied, report.NeedListLength);
                }

                context.UpdateSuccessfulSynchronizationTime();

                if (Log.IsDebugEnabled)
                    Log.Debug("{0} to {1} has finished successfully{2}", work, destinationUrl, moreDetails);
            }
            else
            {
                if (work.IsCancelled || report.Exception is TaskCanceledException)
                {
                    synchronizationCancelled = true;
                    if (Log.IsDebugEnabled)
                        Log.DebugException(string.Format("{0} to {1} was canceled", work, destinationUrl), report.Exception);
                }
                else
                {
                    Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destinationUrl), report.Exception);
                }
            }

            Queue.SynchronizationFinished(work, destinationUrl);

            if (synchronizationCancelled == false)
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

        private IEnumerable<FileHeader> GetFilesToSynchronization(Etag from, int take)
        {
            var filesToSynchronization = new List<FileHeader>();

            if (Log.IsDebugEnabled)
                Log.Debug("Getting files to synchronize with ETag greater than {0} [parameter take = {1}]",
                      from, take);

            try
            {
                storage.Batch(
                    accessor =>
                    filesToSynchronization =
                    accessor.GetFilesAfter(from, take).ToList());
            }
            catch (Exception e)
            {
                Log.WarnException(
                    string.Format("Could not get files to synchronize after: " + from), e);
            }

            return filesToSynchronization;
        }

        private async Task<SynchronizationConfirmation[]> ConfirmPushedFiles(IList<SynchronizationDetails> filesNeedConfirmation, ISynchronizationServerClient synchronizationServerClient)
        {
            if (!filesNeedConfirmation.Any())
                return new SynchronizationConfirmation[0];

            return await synchronizationServerClient.GetConfirmationForFilesAsync(filesNeedConfirmation.Select(x => new Tuple<string, Etag>(x.FileName, x.FileETag))).ConfigureAwait(false);
        }

        private IEnumerable<SynchronizationDetails> GetSyncingConfigurations(SynchronizationDestination destination)
        {
            var configObjects = new List<SynchronizationDetails>();

            try
            {
                storage.Batch(
                    accessor =>
                    {
                        int totalCount;
                        configObjects = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNameForFile(string.Empty, destination.Url), 0, 100, out totalCount)
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

        internal IEnumerable<SynchronizationDestination> GetSynchronizationDestinations()
        {
            var destinationsConfigExists = false;
            storage.Batch(accessor => destinationsConfigExists = accessor.ConfigExists(SynchronizationConstants.RavenSynchronizationDestinations));

            if (!destinationsConfigExists)
            {
                if (failedAttemptsToGetDestinationsConfig < 3 || failedAttemptsToGetDestinationsConfig % 10 == 0)
                {
                    if (Log.IsDebugEnabled)
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
            if (destinationsStrings.Any() == false)
            {
                Log.Warn("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not contain any destination");
                yield break;
            }

            foreach (var token in destinationsStrings)
            {
                yield return token.JsonDeserialization<SynchronizationDestination>();
            }
        }

        private int AvailableSynchronizationRequestsTo(string destinationFileSystemUrl)
        {
            var max = SynchronizationConfigAccessor.GetOrDefault(storage).MaxNumberOfSynchronizationsPerDestination;
            var active = synchronizationQueue.NumberOfActiveSynchronizationsFor(destinationFileSystemUrl);

            return max - active;
        }

        public void Cancel(string fileName)
        {
            if (Log.IsDebugEnabled)
                Log.Debug("Cancellation of active synchronizations of a file '{0}'", fileName);
            Queue.CancelActiveSynchronizations(fileName);
        }

        private static void LogFilesInfo(string message, ICollection<FileHeader> files)
        {
            if (Log.IsDebugEnabled)
                Log.Debug(message, files.Count,
                      string.Join(",", files.Select(x => string.Format("{0} [ETag {1}]", x.FullPath, x.Metadata.Value<Guid>(Constants.MetadataEtagField)))));
        }

        public void Dispose()
        {
            context.StopWork();

            try
            {
                task.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException aggregate)
            {
                if (aggregate.InnerException is TaskCanceledException == false)
                {
                    Log.Warn("Synchronization task stopped with the following exception", aggregate.InnerException);
                }
            }

            context.Dispose();
        }
    }
}
