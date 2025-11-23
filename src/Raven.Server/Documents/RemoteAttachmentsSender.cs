using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TransactionMerger.Commands;
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
        private readonly List<Exception> _exceptions = new List<Exception>();
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
            var directUploaders = new ConcurrentDictionary<string, Lazy<AttachmentUploader>>();
            var uploadTasks = new Task<AttachmentRemoteInfo>[Configuration.ConcurrentUploads ?? DefaultConcurrentThreadsNumber];
            var docsStates = new Dictionary<string, Queue<AttachmentRemoteInfo>>(StringComparer.OrdinalIgnoreCase);

            try
            {
                using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                while (totalCount < maxItemsToProcess)
                {
                    context.Reset();
                    context.Renew();

                    Stopwatch duration;
                    _totalUploaded = 0L;
                    _exceptions.Clear();
                    docsStates.Clear();
                    var processed = new Queue<AttachmentRemoteInfo>();

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

                        Array.Fill(uploadTasks, Task.FromResult<AttachmentRemoteInfo>(null));

                        try
                        {
                            // upload the attachments to cloud and update the document
                            foreach (AttachmentRemoteInfo document in toRemote)
                            {
                                _token.ThrowIfCancellationRequested();

                                if (CanContinueBatch(Logger, duration, _totalUploaded, _token) == false)
                                {
                                    break;
                                }

                                switch (document.Status)
                                {
                                    case BackgroundWorkInfoStatus.Delete:
                                        if (Logger.IsDebugEnabled)
                                            Logger.Debug($"Skipping document with id: '{document.LowerId}'.");

                                        // document or attachment was deleted, need to remove it from remote tree
                                        processed.Enqueue(document);
                                        continue;
                                    case BackgroundWorkInfoStatus.Process:
                                        if (docsStates.TryGetValue(document.Id, out var states))
                                        {
                                            if (states == null)
                                            {
                                                if (Logger.IsDebugEnabled)
                                                    Logger.Debug($"Processing document with id: '{document.LowerId}' without uploading to remote destination, because it is already was processed in this batch.");

                                                // this document was already processed, we add the duplicate to processed queue
                                                processed.Enqueue(document);
                                                continue;
                                            }

                                            // this document is currently being processed, we add it to the duplicates queue
                                            // we will handle its result in HandleUploadTaskResult when the current processing task finish
                                            if (Logger.IsDebugEnabled)
                                                Logger.Debug($"Marking document with id: '{document.LowerId}' as being processed in current batch.");

                                            states.Enqueue(document);

                                            continue;
                                        }

                                        // add duplicates queue for this document which is being processed now
                                        docsStates.Add(document.Id, new Queue<AttachmentRemoteInfo>());

                                        var index = Task.WaitAny(uploadTasks);
                                        try
                                        {
                                            var res = await uploadTasks[index];
                                            HandleUploadTaskResult(res, processed, docsStates);
                                        }
                                        catch (Exception e)
                                        {
                                            _exceptions.Add(e);
                                        }
                                        finally
                                        {
                                            uploadTasks[index] = CreateUploadTaskAsync(directUploaders, currentTime, document);
                                        }

                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException(nameof(document.Status), $"Document '{document.LowerId} had unexpected status '{document.Status}'.");
                                }
                            }
                        }
                        finally
                        {
                            // Wait for all uploads to complete
                            try
                            {
                                await Task.WhenAll(uploadTasks);
                            }
                            catch
                            {
                                // we had a failed task, we will handle it below
                            }

                            foreach (var t in uploadTasks)
                            {
                                _token.ThrowIfCancellationRequested();

                                try
                                {
                                    var res = await t;
                                    HandleUploadTaskResult(res, processed, docsStates);
                                }
                                catch (Exception e)
                                {
                                    _exceptions.Add(e);
                                }
                            }
                        }

                        if (processed.Count == 0)
                        {
                            if (Logger.IsTraceEnabled)
                                Logger.Debug($"Skipping upload attachments of whole batch of '{toRemote.Count:#,#;;0}' documents, Uploaded: {new Size(_totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRemote.Select(x => $"{x.Id}"))}");
                            else if (Logger.IsDebugEnabled)
                                Logger.Debug($"Skipping upload attachments of whole batch of '{toRemote.Count:#,#;;0}' documents, Uploaded: {new Size(_totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'.");

                            if (_exceptions.Count == 0)
                            {
                                continue;
                            }

                            var e = new AggregateException($"Failed to upload all attachments. Failed keys: {string.Join(", ", toRemote.Select(x => x.Id).Distinct(StringComparer.OrdinalIgnoreCase).Select(x => $"{x}"))}", _exceptions);
                            ForTestingPurposes?.BeforeAllBatchFailure?.Invoke(e);

                            // we have exceptions and nothing was uploaded to remote storage, we need to throw
                            throw e;
                        }
                    }

                    var command = new UpdateRemoteAttachmentsCommand(processed, _database, currentTime);
                    await _database.TxMerger.Enqueue(command);

                    ForTestingPurposes?.BeforeEndOfTheBatch?.Invoke(_exceptions);

                    if (Logger.IsInfoEnabled)
                    {
                        var uploadedSizeText = Client.Util.Size.Humane(_totalUploaded);
                        var docsCount = processed.Select(x => x.Id)
                            .Distinct(StringComparer.OrdinalIgnoreCase).Count();
                        var attachmentsCount = command.RemoteCount;
                        var elapsedMs = duration.ElapsedMilliseconds;

                        if (_exceptions.Count == 0)
                        {
                            Logger.Info($"Successfully uploaded {attachmentsCount:#,#;;0} attachments of {docsCount:#,#;;0} documents to remote cloud storage in {elapsedMs:#,#;;0} ms. Total uploaded: {uploadedSizeText}");
                        }
                        else
                        {
                            Logger.Info($"Partially uploaded {attachmentsCount:#,#;;0} attachments of {docsCount:#,#;;0} documents to remote cloud storage in {elapsedMs:#,#;;0} ms. Total uploaded: {uploadedSizeText}. Failed to upload {_exceptions.Count:#,#;;0} attachments:{Environment.NewLine}{new AggregateException(_exceptions)}");
                        }
                    }
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

        private void HandleUploadTaskResult(AttachmentRemoteInfo res, Queue<AttachmentRemoteInfo> processed, Dictionary<string, Queue<AttachmentRemoteInfo>> docStates)
        {
            if (res != null)
            {
                processed.Enqueue(res);

                if (docStates.TryGetValue(res.Id, out var queue))
                {
                    while (queue.TryDequeue(out var item))
                    {
                        // process the items that were waiting for this document to be processed
                        processed.Enqueue(item);
                    }
                }

                docStates[res.Id] = null; // mark as processed

                _totalUploaded += res.AttachmentsSize;
            }
        }

        private async Task<AttachmentRemoteInfo> CreateUploadTaskAsync(ConcurrentDictionary<string, Lazy<AttachmentUploader>> uploaders, DateTime currentTime, AttachmentRemoteInfo document)
        {
            var s = 0L;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (Attachment attachment in _database.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(context, AttachmentType.Document, document.LowerId))
                {
                    _token.ThrowIfCancellationRequested();

                    if (attachment.RemoteParameters.IsRemoteStorageAttachment())
                    {
                        continue;
                    }

                    if (attachment.RemoteParameters != null)
                    {
                        if (currentTime < attachment.RemoteParameters.At)
                            continue;

                        var identifier = attachment.RemoteParameters.Identifier;

                        if (Configuration.Destinations.TryGetValue(identifier, out var destination) == false || destination.HasUploader() == false)
                        {
                            // destination no longer exist or is disabled, skip uploading this attachment
                            continue;
                        }

                        var lazyUploader = uploaders.GetOrAdd(identifier, new Lazy<AttachmentUploader>(() => 
                            new AttachmentUploader(UploaderSettings.GenerateDirectUploaderSettingsForAttachments(_database, identifier, destination.S3Settings, destination.AzureSettings), Logger, _token)));
                        var uploader = lazyUploader.Value;
                        var hash = attachment.Base64Hash.ToString();
                        var objectSizeFromMetadata = await uploader.GetObjectSizeAsync(string.Empty, attachment.Base64Hash.ToString());
                        Stream attachmentStream;
                        if (objectSizeFromMetadata.HasValue)
                        {
                            // The attachment already exists in the cloud, the file name is the hash so we can check if size matches to detect partial uploads
                            attachmentStream = objectSizeFromMetadata == attachment.Size ? null : _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, attachment.Base64Hash);
                        }
                        else
                        {
                            attachmentStream = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, attachment.Base64Hash);
                        }

                        if (attachmentStream == null)
                        {
                            // attachment was deleted or already exist in cloud, need to remove it from remote tree
                            if (Logger.IsDebugEnabled)
                            {
                                Logger.Debug($"Skipping upload of attachment with '{hash}' on {uploader.GetBackupDescription()}.");
                            }
                        }
                        else
                        {
                            await using (attachmentStream)
                            await using (var stream = uploader.StreamForBackupDestination(_database, string.Empty, hash))
                            {
                                if (Logger.IsDebugEnabled)
                                {
                                    Logger.Debug($"Starting the upload of remote attachment '{hash}' on {uploader.GetBackupDescription()}.");
                                }

                                await attachmentStream.CopyToAsync(stream, _token.Token);
                            }

                            s += attachment.Size;
                        }
                    }
                }

                document.AttachmentsSize = s;
                return document;
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
            private readonly Queue<AttachmentRemoteInfo> _remote;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int RemoteCount;

            public UpdateRemoteAttachmentsCommand(Queue<AttachmentRemoteInfo> remote, DocumentDatabase database, DateTime currentTime)
            {
                _remote = remote;
                _database = database;
                _currentTime = currentTime;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                RemoteCount = _database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.ProcessDocuments(context, _remote, _currentTime);

                return RemoteCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new UpdateRemoteAttachmentsCommandDto
                {
                    Remote = _remote.Select(x => (Ticks: x.Ticks, DocumentId: x.LowerId, Id: x.Id, Status: x.Status)).ToArray(),
                    CurrentTime = _currentTime
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
            internal Action<List<Exception>> BeforeEndOfTheBatch;

            internal Action<AggregateException> BeforeAllBatchFailure;
        }
    }

    internal class RemoteAttachmentsStatsScope
    {
        public UploadProgress AzureUpload { get; set; }

        public UploadProgress FtpUpload { get; set; }

        public UploadProgress GlacierUpload { get; set; }

        public UploadProgress GoogleCloudUpload { get; set; }

        public UploadProgress S3Upload { get; set; }

        public int NumberOfAttachments { get; set; }

        public string AttachmentName { get; set; }
    }

    internal sealed class UpdateRemoteAttachmentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand>
    {
        public RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var remote = new Queue<AttachmentRemoteInfo>();
            foreach (var item in Remote)
            {
                remote.Enqueue(new AttachmentRemoteInfo(item.Item1.Clone(context.Allocator), item.Item2.Clone(context.Allocator), item.Item3, item.Item4));
            }
            var command = new RemoteAttachmentsSender.UpdateRemoteAttachmentsCommand(remote, database, CurrentTime);
            return command;
        }

        public (Slice, Slice, string, BackgroundWorkInfoStatus)[] Remote { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
