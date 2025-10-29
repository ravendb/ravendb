using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Attachments;
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
    public class RetireAttachmentsSender : BackgroundWorkBase
    {
        private static readonly int DefaultRetireFrequencyInSec = 60;
        private static readonly int ReadTransactionMaxOpenTimeInMs = 60_000;
        private static readonly long BatchSizeInBytes = PlatformDetails.Is32Bits == false ? 1024 * Constants.Size.Megabyte : 4 * Constants.Size.Megabyte;
        private static readonly Size BatchSizeUnit = new Size(BatchSizeInBytes, SizeUnit.Bytes);
        private static readonly int BatchSize = PlatformDetails.Is32Bits == false ? 36 : 8;
        private static readonly int DefaultConcurrentThreadsNumber = PlatformDetails.Is32Bits == false ? 8 : 2;
        private readonly DocumentDatabase _database;
        private readonly TimeSpan _retirePeriod;
        private readonly OperationCancelToken _token;
        private readonly List<Exception> _exceptions = new List<Exception>();
        private long _totalUploaded;

        private bool _allHalted => Configuration == null || Configuration.Destinations.Count == 0 || Configuration.Destinations.All(x => x.Value.Disabled == true);

        public RetiredAttachmentsConfiguration Configuration { get; }

        internal RetireAttachmentsSender(DocumentDatabase database, RetiredAttachmentsConfiguration retiredAttachmentsConfiguration) : base(database.Name, database.Loggers.GetLogger<RetireAttachmentsSender>(), database.DatabaseShutdown)
        {
            Configuration = retiredAttachmentsConfiguration;
            _database = database;
            _retirePeriod = TimeSpan.FromSeconds(Configuration?.RetireFrequencyInSec ?? DefaultRetireFrequencyInSec);
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
                    await WaitOrThrowOperationCanceled(_retirePeriod);
                    await RetireAttachments(BatchSize, Configuration.MaxItemsToProcess ?? ExpiredDocumentsCleaner.DefaultMaxItemsToProcessInSingleRun);
                }
            });
            return t;
        }

        internal async Task<int> RetireAttachments(int batchSize, long maxItemsToProcess)
        {
            if (Configuration.HasUploader() == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot retire attachments on '{_database.Name}' because no destination is configured.");
                return 0;
            }

            var totalCount = 0;
            var currentTime = _database.Time.GetUtcNow();
            var directUploaders = new Dictionary<string, AttachmentUploader>();

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
                    var processed = new Queue<DocumentExpirationInfo>();

                    using (var tx = context.OpenReadTransaction())
                    using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(context))
                    {
                        DatabaseTopology topology;

                        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                        using (serverContext.OpenReadTransaction())
                        {
                            topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                        }

                        var options = new BackgroundWorkParameters(context, currentTime, topology, _database.ServerStore.NodeTag, AmountToTake: batchSize, MaxItemsToProcess: maxItemsToProcess);

                        var toRetire = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out duration, _token.Token);

                        if (toRetire == null || toRetire.Count == 0)
                        {
                            return totalCount;
                        }

                        var uploadTasks = Enumerable.Repeat(Task.CompletedTask, Configuration.ConcurrentUploads ?? DefaultConcurrentThreadsNumber).ToArray();
                        var uploadedItems = new ConcurrentQueue<DocumentExpirationInfo>();

                        try
                        {
                            // upload the attachments to cloud and update the document
                            foreach (var doc in toRetire)
                            {
                                var identifier = doc.Id;
                                _token.ThrowIfCancellationRequested();

                                if (CanContinueBatch(Logger, duration, _totalUploaded, _token) == false)
                                {
                                    break;
                                }

                                if (string.IsNullOrEmpty(identifier))
                                {
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug($"Skipping PutRetire of retired attachment with key: '{doc.LowerId}' because it's identifier '{identifier}' IsNullOrEmpty.");

                                    // document or attachment was deleted, need to remove it from retired tree
                                    processed.Enqueue(doc);
                                    continue;
                                }

                                if (directUploaders.TryGetValue(identifier, out var directUpload) == false)
                                {
                                    // get the destination configuration for the identifier & create new uploader for the attachment
                                    var destination = Configuration.Destinations[identifier];
                                    directUpload = new AttachmentUploader(UploaderSettings.GenerateDirectUploaderSettingsForAttachments(_database, identifier, destination.S3Settings, destination.AzureSettings), Logger, _token);

                                    directUploaders[identifier] = directUpload;
                                }

                                var index = Task.WaitAny(uploadTasks);
                                var t = uploadTasks[index];
                                if (t.IsFaulted)
                                {
                                    _exceptions.Add(t.Exception);
                                }

                                uploadTasks[index] = CreateUploadTaskAsync(directUpload, uploadedItems, doc);
                            }
                        }
                        finally
                        {
                            // Wait for all uploads to complete
                            await Task.WhenAll(uploadTasks);
                            foreach (var t in uploadTasks)
                            {
                                if (t.IsFaulted)
                                {
                                    _exceptions.Add(t.Exception);
                                }
                            }
                            while (uploadedItems.TryDequeue(out var item))
                            {
                                processed.Enqueue(item);
                            }
                        }

                        if (toRetire.Count != processed.Count)
                        {
                            // we had skipped items
                            if (Logger.IsDebugEnabled)
                                Logger.Debug($"Skipping retiring of '{toRetire.Count - processed.Count:#,#;;0}' attachments, Uploaded: {new Size(_totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => processed.Contains(x) == false))}");
                        }

                        if (processed.Count == 0)
                        {
                            if (Logger.IsDebugEnabled)
                                Logger.Debug($"Skipping retiring whole batch of '{toRetire.Count:#,#;;0}' attachments, Uploaded: {new Size(_totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => processed.Contains(x) == false))}");

                            if (_exceptions.Count == 0)
                            {
                                continue;
                            }

                            // we have exceptions and nothing was retired, we need to throw
                            throw new AggregateException("Failed to upload all attachments.", _exceptions);
                        }
                    }

                    var command = new UpdateRetiredAttachmentsCommand(processed, _database, currentTime);
                    await _database.TxMerger.Enqueue(command);

                    if (Logger.IsInfoEnabled)
                    {
                        var uploadedSizeText = Client.Util.Size.Humane(_totalUploaded);
                        var retiredCount = command.RetiredCount;
                        var elapsedMs = duration.ElapsedMilliseconds;

                        if (_exceptions.Count == 0)
                        {
                            Logger.Info($"Successfully retired {retiredCount:#,#;;0} attachments in {elapsedMs:#,#;;0} ms. Total uploaded: {uploadedSizeText}");
                        }
                        else
                        {
                            Logger.Info($"Partially retired {retiredCount:#,#;;0} attachments in {elapsedMs:#,#;;0} ms. Total uploaded: {uploadedSizeText}. Failed to upload {_exceptions.Count:#,#;;0} attachments:{Environment.NewLine}{new AggregateException(_exceptions)}");
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
                    Logger.Error($"Failed to retire attachments on '{_database.Name}' which are older than '{currentTime}'.", e);
            }
            return totalCount;
        }

        private async Task CreateUploadTaskAsync(AttachmentUploader uploader, ConcurrentQueue<DocumentExpirationInfo> concurrentItems, DocumentExpirationInfo doc)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.ExtractHashSliceFromAttachmentId(context, doc.LowerId, out Slice hashSlice))
            using (context.OpenReadTransaction())
            {
                var hash = hashSlice.ToString();
                var attachmentStream = GetAttachmentStreamAndStreamLength(context, hashSlice, await uploader.GetObjectSizeAsync(string.Empty, hash), out var attachmentLength);
                
                if (attachmentStream == null)
                {
                    // attachment was deleted or already exist in cloud, need to remote it from retired tree
                    concurrentItems.Enqueue(doc);

                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug($"Skipping upload of attachment with '{hash}' on {uploader.GetBackupDescription()}.");
                    }
                }
                else
                {
                    _token.ThrowIfCancellationRequested();

                    await using (attachmentStream)
                    await using (var stream = uploader.StreamForBackupDestination(_database, string.Empty, hash))
                    {
                        if (Logger.IsDebugEnabled)
                        {
                            Logger.Debug($"Starting the upload of retired attachment '{hash}' on {uploader.GetBackupDescription()}.");
                        }

                        await attachmentStream.CopyToAsync(stream, _token.Token);
                    }

                    Interlocked.Add(ref _totalUploaded, attachmentLength);
                    concurrentItems.Enqueue(doc);
                }
            }
        }

        private Stream GetAttachmentStreamAndStreamLength(DocumentsOperationContext context, Slice hashSlice, long? objectSizeFromMetadata, out long attachmentLength)
        {
            if (objectSizeFromMetadata.HasValue)
            {
                // The attachment already exists in the cloud, the file name is the hash so we can check if size matches to detect partial uploads
                attachmentLength = AttachmentsStorage.GetAttachmentStreamLength(context, hashSlice);
                if (objectSizeFromMetadata == attachmentLength)
                {
                    return null;
                }

                return _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, hashSlice);
            }

            (Stream attachmentStream, attachmentLength) = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStreamAndLength(context, hashSlice);
            return attachmentStream;
        }

        internal static bool CanContinueBatch(RavenLogger logger, Stopwatch duration, long totalUploaded, OperationCancelToken token)
        {
            if (duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop handling retired attachments to cloud due to long read tx open time: '{duration.ElapsedMilliseconds}'.");

                return false;
            }

            if (totalUploaded >= BatchSizeInBytes)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Stop handling retired attachments to cloud due to high batch size, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)} / Allowed: {BatchSizeUnit}.");

                return false;
            }

            if (token.Token.IsCancellationRequested)
                return false;

            return true;
        }

        internal sealed class UpdateRetiredAttachmentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Queue<DocumentExpirationInfo> _retired;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int RetiredCount;

            public UpdateRetiredAttachmentsCommand(Queue<DocumentExpirationInfo> retired, DocumentDatabase database, DateTime currentTime)
            {
                _retired = retired;
                _database = database;
                _currentTime = currentTime;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                RetiredCount = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.ProcessDocuments(context, _retired, _currentTime);

                return RetiredCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new UpdateRetiredAttachmentsCommandDto
                {
                    Retired = _retired.Select(x => (Ticks: x.Ticks, LowerId: x.LowerId, Id: x.Id, Status: x.Status)).ToArray(),
                    CurrentTime = _currentTime
                };
            }
        }
    }

    internal class RetireAttachmentsStatsScope
    {
        public UploadProgress AzureUpload { get; set; }

        public UploadProgress FtpUpload { get; set; }

        public UploadProgress GlacierUpload { get; set; }

        public UploadProgress GoogleCloudUpload { get; set; }

        public UploadProgress S3Upload { get; set; }

        public int NumberOfAttachments { get; set; }

        public string AttachmentName { get; set; }
    }

    internal sealed class UpdateRetiredAttachmentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RetireAttachmentsSender.UpdateRetiredAttachmentsCommand>
    {
        public RetireAttachmentsSender.UpdateRetiredAttachmentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var retired = new Queue<DocumentExpirationInfo>();
            foreach (var item in Retired)
            {
                retired.Enqueue(new DocumentExpirationInfo(item.Item1.Clone(context.Allocator), item.Item2.Clone(context.Allocator), item.Item3, item.Item4));
            }
            var command = new RetireAttachmentsSender.UpdateRetiredAttachmentsCommand(retired, database, CurrentTime);
            return command;
        }

        public (Slice, Slice, string, DocumentExpirationInfoStatus)[] Retired { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
