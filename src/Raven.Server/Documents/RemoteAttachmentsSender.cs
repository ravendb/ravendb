using System;
using System.Collections.Concurrent;
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
        private readonly Dictionary<string, Dictionary<string, UploadAttachmentException>> _batchExceptionsByIdentifier = new(StringComparer.OrdinalIgnoreCase);

        // Identifier (case in-sensitive) -> (Hash (case sensitive) -> RetryCount): we keep track of how many times we retried uploading an attachment
        private readonly Dictionary<string, Dictionary<string, long>> _inMemoryStateErrorsByIdentifier = new(StringComparer.OrdinalIgnoreCase);

        // Identifier (case in-sensitive) -> AttachmentUploader
        private readonly Dictionary<string, AttachmentUploader> _uploaderByIdentifier = new(StringComparer.OrdinalIgnoreCase);

        // LowerDocumentId Slice (case in-sensitive) -> Ticks Slice: we keep track of all documents we have seen
        private readonly Dictionary<Slice, List<Slice>> _alreadySeenDocs = new(SliceComparer.Instance);

        // Identifier (case in-sensitive) -> (Hash (case sensitive) -> AttachmentRemoteInfo): we keep track of all attachments to upload
        private readonly Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> _attachmentsToUploadByIdentifier = new(StringComparer.OrdinalIgnoreCase);

        private long _totalUploadedInBytes;

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

            try
            {
                using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                while (totalCount < maxItemsToProcess)
                {
                    ResetBatch(context);

                    using (context.OpenReadTransaction())
                    {
                        DatabaseTopology topology;

                        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                        using (serverContext.OpenReadTransaction())
                        {
                            topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                        }

                        var options = new BackgroundWorkParameters(context, currentTime, topology, _database.ServerStore.NodeTag, AmountToTake: batchSize, MaxItemsToProcess: maxItemsToProcess);

                        var toRemote = _database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDocuments(options, ref totalCount, out Stopwatch duration, _token.Token);

                        if (toRemote == null || toRemote.Count == 0)
                        {
                            return totalCount;
                        }

                        // gather the attachments to upload
                        ConcurrentQueue<AttachmentRemoteInfo> attachmentsToUpload = PrepareAttachmentsBatchToUpload(context, toRemote, duration, currentTime);

                        if (Logger.IsDebugEnabled)
                        {
                            Logger.Debug($"Prepared batch of {attachmentsToUpload.Count} remote attachments to upload.");
                        }

                        // upload the attachments to cloud
                        await UploadAttachmentsBatch(attachmentsToUpload);
                    }

                    AlertAndLogOnBatchErrorsIfNeeded();

                    var command = new UpdateRemoteAttachmentsCommand(_database, currentTime, _alreadySeenDocs, _attachmentsToUploadByIdentifier);
                    await _database.TxMerger.Enqueue(command);

                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug($"Processed remote attachments batch. Uploaded: {new Size(_totalUploadedInBytes, SizeUnit.Bytes)}, Remote Count: {command.RemoteCount}.");
                    }

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

        private void ResetBatch(DocumentsOperationContext context)
        {
            context.Reset();
            context.Renew();

            _totalUploadedInBytes = 0L;
            _batchExceptionsByIdentifier.Clear();
            _alreadySeenDocs.Clear();
            _attachmentsToUploadByIdentifier.Clear();
        }

        private async Task UploadAttachmentsBatch(ConcurrentQueue<AttachmentRemoteInfo> attachmentsToUpload)
        {
            var errors = new ConcurrentQueue<AttachmentRemoteInfo>();
            var uploadTasks = new Task<long>[Configuration.ConcurrentUploads ?? DefaultConcurrentThreadsNumber];
            for (int i = 0; i < uploadTasks.Length; i++)
            {
                uploadTasks[i] = UploadQueueAsync();
            }

            foreach (Task<long> uploadTask in uploadTasks)
            {
                _totalUploadedInBytes += await uploadTask;
            }

            while (errors.TryDequeue(out AttachmentRemoteInfo info))
            {
                HandleUploadTaskException(info);
            }

            async Task<long> UploadQueueAsync()
            {
                long total = 0;
                while (attachmentsToUpload.TryDequeue(out var info))
                {
                    _token.ThrowIfCancellationRequested();

                    try
                    {
                        total += await CreateUploadTaskAsync(info.AttachmentUploader, info.Hash);
                    }
                    catch (Exception e)
                    {
                        info.Exception = e;
                        errors.Enqueue(info);
                    }
                }

                return total;
            }
        }

        private ConcurrentQueue<AttachmentRemoteInfo> PrepareAttachmentsBatchToUpload(DocumentsOperationContext context, Queue<DocumentExpirationInfo> toRemote, Stopwatch duration, DateTime currentTime)
        {
            var attachmentsToUpload = new ConcurrentQueue<AttachmentRemoteInfo>();

            var bytesToUpload = 0L;
            foreach (DocumentExpirationInfo document in toRemote)
            {
                _token.ThrowIfCancellationRequested();

                if (CanContinueBatch(Logger, duration, bytesToUpload, _token) == false)
                {
                    break;
                }

                // we add all the ticks for this document, so we can update them all at once

                ref var ticks = ref CollectionsMarshal.GetValueRefOrAddDefault(_alreadySeenDocs, document.LowerId, out var exists);
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
                        // in this switch case the document.Id is only used for logging or alerts!
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

                            if (_uploaderByIdentifier.TryGetValue(identifier, out var uploader) == false)
                            {
                                uploader = new AttachmentUploader(UploaderSettings.GenerateDirectUploaderSettingsForAttachments(_database, identifier, destination.S3Settings, destination.AzureSettings), Logger, _token);
                                _uploaderByIdentifier[identifier] = uploader;
                            }

                            var hash = attachment.Base64Hash.ToString();
                            var attachmentByHashForIdentifier = _attachmentsToUploadByIdentifier.GetOrAdd(identifier);
                            if (attachmentByHashForIdentifier.TryGetValue(hash, out var info) is false)
                            {
                                info = new AttachmentRemoteInfo
                                {
                                    Hash = hash,
                                    AttachmentUploader = uploader
                                };
                                attachmentsToUpload.Enqueue(info);
                                attachmentByHashForIdentifier[hash] = info;
                                bytesToUpload += attachment.Size;
                            }
                            info.DocumentIds.Add(document.Id);

                            if (Logger.IsDebugEnabled)
                            {
                                Logger.Debug($"Added a remote attachment '{attachment.Name}' with hash '{hash}' for document id '{document.Id}' to upload batch for identifier '{identifier}'.");
                            }
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(document.Status), $"Document '{document.LowerId} had unexpected status '{document.Status}'.");
                }
            }

            return attachmentsToUpload;
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

        private void HandleUploadTaskException(AttachmentRemoteInfo info)
        {
            var hash = info.Hash;
            string identifier = info.AttachmentUploader.Identifier;
            var errors = _inMemoryStateErrorsByIdentifier.GetOrAdd(identifier);

            var count = errors.GetOrAdd(hash);
            if (count < 3)
            {
                errors[hash] = ++count;

                info.Status = BackgroundWorkInfoStatus.Retry;
            }
            else
            {
                // we have tried enough times, we need to remove it from background tree and alert
                var ex = new UploadAttachmentException($"Failed to upload remote attachment with identifier '{identifier}' and hash '{hash}', the attachment belong to following documents: {string.Join(", ", info.DocumentIds.Select(x => $"{x}"))}", info.Exception);
                _batchExceptionsByIdentifier.GetOrAdd(identifier).TryAdd(hash, ex);
                errors.Remove(hash);
                info.Status = BackgroundWorkInfoStatus.Skip;
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

        internal static bool CanContinueBatch(RavenLogger logger, Stopwatch duration, long totalBytesToUpload, OperationCancelToken token)
        {
            if (duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop gathering remote attachments to upload due to long read tx open time: '{duration.ElapsedMilliseconds}'.");

                return false;
            }

            if (totalBytesToUpload >= BatchSizeInBytes)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop gathering remote attachments to upload due to high batch size, Uploaded: {new Size(totalBytesToUpload, SizeUnit.Bytes)} / Allowed: {BatchSizeUnit}.");

                return false;
            }

            if (token.Token.IsCancellationRequested)
                return false;

            return true;
        }

        internal sealed class UpdateRemoteAttachmentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Dictionary<Slice, List<Slice>> _seenDocs;
            private readonly Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> _attachmentsToUploadByIdentifier;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int RemoteCount;

            public UpdateRemoteAttachmentsCommand(DocumentDatabase database, DateTime currentTime, Dictionary<Slice, List<Slice>> seenDocs, Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> attachmentsToUploadByIdentifier)
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
            var seenDocs = new Dictionary<Slice, List<Slice>>();
            foreach (var item in SeenDocs)
            {
                seenDocs[item.Key.Clone(context.Allocator)] = item.Value.Select(slice => slice.Clone(context.Allocator)).ToList();
            }

            var command = new RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand(database, CurrentTime, seenDocs, AttachmentsToUploadByIdentifier);
            return command;
        }

        public Dictionary<Slice, List<Slice>> SeenDocs { get; set; }

        public Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> AttachmentsToUploadByIdentifier { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
