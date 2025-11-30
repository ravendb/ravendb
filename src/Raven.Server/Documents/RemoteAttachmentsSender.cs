using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Exceptions.Attachments;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Voron;
using Voron.Global;

namespace Raven.Server.Documents
{
    public class RemoteAttachmentsSender : BackgroundWorkBase
    {
        private static readonly int DefaultRemoteFrequencyInSec = 60;
        private static readonly int ReadTransactionMaxOpenTimeInMs = 60_000;
        private static readonly long BatchSizeInBytes = PlatformDetails.Is32Bits == false ? 1024 * Constants.Size.Megabyte : 4 * Constants.Size.Megabyte;
        private static readonly Size BatchSizeUnit = new Size(BatchSizeInBytes, SizeUnit.Bytes);
        private static readonly int BatchSize = PlatformDetails.Is32Bits == false ? 36 : 8;
        private static readonly int DefaultConcurrentThreadsNumber = PlatformDetails.Is32Bits == false ? 8 : 2;
        private readonly DocumentDatabase _database;
        private readonly TimeSpan _remotePeriod;
        private readonly OperationCancelToken _token;

        // Identifier (case in-sensitive) -> (Hash (case sensitive) -> Exception): we keep the exceptions to alert at the end of the batch
        private readonly Dictionary<string, Dictionary<string, UploadAttachmentException>> _batchExceptionsByIdentifier = new Dictionary<string, Dictionary<string, UploadAttachmentException>>(StringComparer.OrdinalIgnoreCase);
        // Identifier (case in-sensitive) -> (Hash (case sensitive) -> RetryCount): we keep track of how many times we retried uploading an attachment
        private readonly Dictionary<string, Dictionary<string, long>> _inMemoryStateErrorsByIdentifier = new Dictionary<string, Dictionary<string, long>>(StringComparer.OrdinalIgnoreCase);
        private long _totalUploaded;

        private bool _allHalted => Configuration == null || Configuration.Disabled || Configuration.Destinations == null || Configuration.Destinations.Count == 0 || Configuration.Destinations.All(x => x.Value.Disabled == true);

        public RemoteAttachmentsConfiguration Configuration { get; }

        internal RemoteAttachmentsSender(DocumentDatabase database, RemoteAttachmentsConfiguration remoteAttachmentsConfiguration) : base(database.Name, database.Loggers.GetLogger<RemoteAttachmentsSender>(), database.DatabaseShutdown)
        {
            Configuration = remoteAttachmentsConfiguration;
            _database = database;
            _remotePeriod = TimeSpan.FromSeconds(Configuration?.CheckFrequencyInSec ?? DefaultRemoteFrequencyInSec);
            _token = new OperationCancelToken(CancellationToken);
        }

        protected override Task DoWork()
        {
            if (_allHalted)
                return Task.CompletedTask;

            var t = Task.Run(async () =>
            {
                while (_allHalted == false)
                {
                    await WaitOrThrowOperationCanceled(_remotePeriod);
                    await ProcessRemoteAttachments(BatchSize, Configuration.MaxItemsToProcess ?? ExpiredDocumentsCleaner.DefaultMaxItemsToProcessInSingleRun);
                }
            });
            return t;
        }

        internal async Task<int> ProcessRemoteAttachments(int batchSize, long maxItemsToProcess)
        {
            if (Configuration.HasDestination() == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot process remote attachments on '{_database.Name}' because no destination is configured.");
                return 0;
            }

            var totalCount = 0;
            var currentTime = _database.Time.GetUtcNow();
            var directUploaders = new Dictionary<string, AttachmentUploader>(StringComparer.OrdinalIgnoreCase);
            var uploadTasks = new Task<long>[Configuration.ConcurrentUploads ?? DefaultConcurrentThreadsNumber];
            var uploadTaskMetadata = new Dictionary<Task, (string Identifier, KeyValuePair<string, AttachmentRemoteInfo> MetadataPerHash)>();
            var alreadySeenDocs = new Dictionary<string, List<Slice>>(StringComparer.OrdinalIgnoreCase);
            var attachmentsToUploadByIdentifier = new Dictionary<string, Dictionary<string, AttachmentRemoteInfo>>(StringComparer.OrdinalIgnoreCase);

            try
            {
                using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                while (totalCount < maxItemsToProcess)
                {
                    context.Reset();
                    context.Renew();

                    Stopwatch duration;
                    _totalUploaded = 0L;
                    _batchExceptionsByIdentifier.Clear();
                    alreadySeenDocs.Clear();
                    attachmentsToUploadByIdentifier.Clear();
                    uploadTaskMetadata.Clear();

                    using (var tx = context.OpenReadTransaction())
                    using (_database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.Initialize(context))
                    {
                        DatabaseTopology topology;

                        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                        using (serverContext.OpenReadTransaction())
                        {
                            topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                        }

                        var options = new BackgroundWorkParameters(context, currentTime, topology, _database.ServerStore.NodeTag, AmountToTake: batchSize, MaxItemsToProcess: maxItemsToProcess);

                        var toRemote = _database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDocuments(options, ref totalCount, out duration, _token.Token);

                        if (toRemote == null || toRemote.Count == 0)
                        {
                            return totalCount;
                        }

                        Array.Fill(uploadTasks, Task.FromResult<long>(0L));

                        try
                        {
                            // upload the attachments to cloud and update the document
                            foreach (DocumentExpirationInfo document in toRemote)
                            {
                                _token.ThrowIfCancellationRequested();

                                if (CanContinueBatch(Logger, duration, _totalUploaded, _token) == false)
                                {
                                    break;
                                }

                                // we add all the ticks for this document, so we can update them all at once

                                ref var ticks = ref CollectionsMarshal.GetValueRefOrAddDefault(alreadySeenDocs, document.Id, out var exists);
                                if (exists)
                                {
                                    ticks.Add(document.Ticks);
                                    continue;
                                }

                                ticks = [document.Ticks];

                                switch (document.Status)
                                {
                                    case BackgroundWorkInfoStatus.Delete:
                                        if (Logger.IsDebugEnabled)
                                            Logger.Debug($"Skipping document with id: '{document.LowerId}'.");

                                        // document or attachment was deleted, we will remove it from remote tree
                                        continue;
                                    case BackgroundWorkInfoStatus.Process:
                                        foreach (Attachment attachment in _database.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(context, AttachmentType.Document, document.LowerId))
                                        {
                                            _token.ThrowIfCancellationRequested();

                                            if (attachment.RemoteParameters is null)
                                                continue;

                                            if (attachment.RemoteParameters.IsRemoteStorageAttachment())
                                                continue;

                                            if (currentTime < attachment.RemoteParameters.At)
                                                continue;

                                            var identifier = attachment.RemoteParameters.Identifier;

                                            if (Configuration.Destinations.TryGetValue(identifier, out var destination) == false || destination.HasUploader() == false)
                                            {
                                                // destination no longer exist or is disabled, skip uploading this attachment
                                                HandleSkippedItem(document.Id, attachment, destination);
                                                continue;
                                            }

                                            if (directUploaders.TryGetValue(identifier, out var uploader) == false)
                                            {
                                                uploader = new AttachmentUploader(UploaderSettings.GenerateDirectUploaderSettingsForAttachments(_database, identifier, destination.S3Settings, destination.AzureSettings), Logger, _token);
                                                directUploaders[identifier] = uploader;
                                            }

                                            var info = attachmentsToUploadByIdentifier
                                                .GetOrAdd(identifier) // identifier is case-insensitive
                                                .GetOrAdd(attachment.Base64Hash.ToString()); // hash is case-sensitive and GetOrAdd() extension method creates the Dictionary with default comparer which is StringComparer.Ordinal by default
                                           
                                            info.DocumentIds.Add(document.Id);
                                            info.AttachmentUploader = uploader;
                                        }

                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException(nameof(document.Status), $"Document '{document.LowerId} had unexpected status '{document.Status}'.");
                                }
                            }

                            foreach (var (identifier, hashesToUpload) in attachmentsToUploadByIdentifier)
                            {
                                foreach (KeyValuePair<string, AttachmentRemoteInfo> kvp in hashesToUpload)
                                {
                                    _token.ThrowIfCancellationRequested();

                                    var hash = kvp.Key;
                                    var index = Task.WaitAny(uploadTasks);
                                    Task<long> pendingTask = uploadTasks[index];
                                    try
                                    {
                                        var res = await pendingTask;
                                        HandleUploadTaskResult(res);
                                    }
                                    catch (Exception e)
                                    {
                                        uploadTaskMetadata.TryGetValue(pendingTask, out var metadata);
                                        HandleUploadTaskException(e, metadata);
                                    }
                                    finally
                                    {
                                        uploadTaskMetadata.Remove(pendingTask);

                                        Task<long> newTask = CreateUploadTaskAsync(kvp.Value.AttachmentUploader, hash);
                                        uploadTasks[index] = newTask;
                                        uploadTaskMetadata[newTask] = (identifier, kvp);
                                    }
                                }
                            }
                        }
                        finally
                        {
                            for (int index = 0; index < uploadTasks.Length; index++)
                            {
                                _token.ThrowIfCancellationRequested();
                                Task<long> pendingTask = uploadTasks[index];
                                try
                                {
                                    var res = await pendingTask;
                                    HandleUploadTaskResult(res);
                                }
                                catch (Exception e)
                                {
                                    uploadTaskMetadata.TryGetValue(pendingTask, out var metadata);
                                    HandleUploadTaskException(e, metadata);
                                }
                                finally
                                {
                                    uploadTaskMetadata.Remove(pendingTask);
                                }
                            }
                        }
                    }

                    AlertAndLogOnBatchErrorsIfNeeded();

                    var command = new UpdateRemoteAttachmentsCommand(_database, currentTime, alreadySeenDocs, attachmentsToUploadByIdentifier);
                    await _database.TxMerger.Enqueue(command);

                    ForTestingPurposes?.BeforeEndOfTheBatch?.Invoke(_batchExceptionsByIdentifier);
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to process remote attachments on '{_database.Name}' which are older than '{currentTime}'.", e);
            }
            return totalCount;
        }

        private void AlertAndLogOnBatchErrorsIfNeeded()
        {
            if (_batchExceptionsByIdentifier.Count <= 0) 
                return;

            foreach (var (identifier, hashPerError) in _batchExceptionsByIdentifier)
            {
                _token.ThrowIfCancellationRequested();

                var hashes = new HashSet<string>();
                var exceptions = new List<Exception>(hashPerError.Count);
                
                foreach ((string key, UploadAttachmentException exception) in hashPerError)
                {
                    hashes.Add(key);
                    exceptions.Add(exception);
                }

                var msg = $"Failed to upload remote attachment for identifier '{identifier}' after multiple attempts. Skipping further attempts for this attachment. Please check the {nameof(RemoteAttachmentsConfiguration)}.{nameof(RemoteAttachmentsConfiguration.Destinations)} configuration for '{identifier}'.";

                if (Logger.IsDebugEnabled)
                {
                    AggregateException ex = new AggregateException($"Failed to upload remote attachment for identifier '{identifier}' too many times.", exceptions);
                    Logger.Debug(ex, "{0}{1}Failed Hashes: {2}", msg, Environment.NewLine, string.Join(", ", hashes));
                }

                var alert = AlertRaised.Create(_database.Name, AlertTitleError, msg, AlertReason.Attachments_RemoteAttachmentErroredIdentifier, NotificationSeverity.Error, key: nameof(AlertReason.Attachments_RemoteAttachmentErroredIdentifier));
                _database.NotificationCenter.Add(alert);
            }
        }

        private const string AlertTitleSkip = "Remote attachment upload was skipped.";
        private const string AlertTitleError = "Remote attachment upload failed.";
        private long _counter = 0;
        private DateTime _lastAlertTime = DateTime.MinValue;
        private static readonly long AlertThresholdTicks = TimeSpan.FromMinutes(1).Ticks;

        private void HandleSkippedItem(string documentId, Attachment item, RemoteAttachmentsDestinationConfiguration destination)
        {
            var now = _database.Time.GetUtcNow();
            var timeSinceLastAlert = now.Ticks - _lastAlertTime.Ticks;

            // on first skip or each 16th time or if last alert was more than a minute ago
            var shouldAlert = (_counter++ % 16 == 0) || (timeSinceLastAlert > AlertThresholdTicks);

            if (shouldAlert == false && Logger.IsDebugEnabled == false)
            {
                return;
            }

            var destinationStr = destination == null ? "destination is null." : destination.Disabled ? "destination is disabled." : "destination doesn't have uploader configured.";
            var msg = $"Skipping uploading remote attachment '{item.Name}' with identifier '{item.RemoteParameters.Identifier}' for document id '{documentId}'. Reason: {destinationStr}";

            if (Logger.IsDebugEnabled)
            {
                Logger.Debug(msg);
            }

            if (shouldAlert)
            {
                // update the last alert time only if we are sending an alert
                _lastAlertTime = now;
                var alert = AlertRaised.Create(_database.Name, AlertTitleSkip, msg, AlertReason.Attachments_RemoteAttachmentWithoutIdentifier, NotificationSeverity.Warning, key: nameof(AlertReason.Attachments_RemoteAttachmentWithoutIdentifier));
                _database.NotificationCenter.Add(alert);
            }
        }

        private void HandleUploadTaskResult(long res)
        {
            _totalUploaded += res;
        }

        private void HandleUploadTaskException(Exception e, (string Identifier, KeyValuePair<string, AttachmentRemoteInfo> MetadataPerHash) tuple)
        {
            var hash = tuple.MetadataPerHash.Key;
            var errors = _inMemoryStateErrorsByIdentifier.GetOrAdd(tuple.Identifier);

            var count = errors.GetOrAdd(hash);
            if (count < 3)
            {
                errors[hash] = ++count;

                tuple.MetadataPerHash.Value.Status = BackgroundWorkInfoStatus.Retry;
            }
            else
            {
                // we have tried enough times, we need to remove it from background tree and alert
                var ex = new UploadAttachmentException($"Failed to upload remote attachment with identifier '{tuple.Identifier}' and hash '{hash}', the attachment belong to following documents: {string.Join(", ", tuple.MetadataPerHash.Value.DocumentIds.Select(x => $"{x}"))}", e);
                _batchExceptionsByIdentifier.GetOrAdd(tuple.Identifier).TryAdd(hash, ex);
                errors.Remove(hash);
                tuple.MetadataPerHash.Value.Status = BackgroundWorkInfoStatus.Skip;
            }
        }

        private async Task<long> CreateUploadTaskAsync(AttachmentUploader uploader, string hash)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (Slice.From(context.Allocator, hash, out Slice hashSlice))
            using (context.OpenReadTransaction())
            {
                var objectSizeFromMetadata = await uploader.GetObjectSizeAsync(string.Empty, hash);
                if (objectSizeFromMetadata.HasValue)
                {
                    // The attachment already exists in the cloud, the file name is the hash so we can check if size matches to detect partial uploads
                    long attachmentLength = AttachmentsStorage.GetAttachmentStreamLength(context, hashSlice);
                    if (objectSizeFromMetadata == attachmentLength)
                    {
                        if (Logger.IsDebugEnabled)
                        {
                            Logger.Debug($"Skipping upload of attachment with '{hash}' on {uploader.GetBackupDescription()}, attachment already exists.");
                        }
                        return 0;
                    }
                }
                
                await using var attachmentStream = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, hashSlice);
                if (attachmentStream == null)
                {
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug($"Skipping upload of attachment with '{hash}' on {uploader.GetBackupDescription()}, it was deleted locally since we scheduled its upload");
                    }

                    return 0;
                }

                _token.ThrowIfCancellationRequested();

                await using (var stream = uploader.StreamForBackupDestination(_database, string.Empty, hash))
                {
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug($"Starting the upload of remote attachment '{hash}' on {uploader.GetBackupDescription()}.");
                    }

                    await attachmentStream.CopyToAsync(stream, _token.Token);

                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug($"Completed upload of remote attachment '{hash}' on {uploader.GetBackupDescription()}.");
                    }
                    return attachmentStream.Length;
                }
            }
        }

        internal static bool CanContinueBatch(RavenLogger logger, Stopwatch duration, long totalUploaded, OperationCancelToken token)
        {
            if (duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop processing remote attachments upload to cloud due to long read tx open time: '{duration.ElapsedMilliseconds}'.");

                return false;
            }

            if (totalUploaded >= BatchSizeInBytes)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop processing remote attachments upload to cloud due to high batch size, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)} / Allowed: {BatchSizeUnit}.");

                return false;
            }

            if (token.Token.IsCancellationRequested)
                return false;

            return true;
        }

        internal sealed class UpdateRemoteAttachmentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Dictionary<string, List<Slice>> _seenDocs;
            private readonly Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> _attachmentsToUploadByIdentifier;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int RemoteCount;

            public UpdateRemoteAttachmentsCommand(DocumentDatabase database, DateTime currentTime, Dictionary<string, List<Slice>> seenDocs, Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> attachmentsToUploadByIdentifier)
            {
                _seenDocs = seenDocs;
                _attachmentsToUploadByIdentifier = attachmentsToUploadByIdentifier;
                _database = database;
                _currentTime = currentTime;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                RemoteCount = _database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.ProcessRemoteAttachments(context, _currentTime, _seenDocs, _attachmentsToUploadByIdentifier);

                return RemoteCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new UpdateRemoteAttachmentsCommandDto
                {
                    CurrentTime = _currentTime,
                    SeenDocs = _seenDocs,
                    AttachmentsToUploadByIdentifier = _attachmentsToUploadByIdentifier
                };
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action<Dictionary<string, Dictionary<string, UploadAttachmentException>>> BeforeEndOfTheBatch;
        }
    }

    internal sealed class UpdateRemoteAttachmentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand>
    {
        public RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            // clone slices
            var seenDocs = new Dictionary<string, List<Slice>>();
            foreach (var item in SeenDocs)
            {
                seenDocs[item.Key] = item.Value.Select(slice => slice.Clone(context.Allocator)).ToList();
            }

            var command = new RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand(database, CurrentTime, seenDocs, AttachmentsToUploadByIdentifier);
            return command;
        }

        public Dictionary<string, List<Slice>> SeenDocs { get; set; }

        public Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> AttachmentsToUploadByIdentifier { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
