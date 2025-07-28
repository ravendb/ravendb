using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Voron;
using Voron.Global;
using static Raven.Server.Documents.AbstractBackgroundWorkStorage;
using static Raven.Server.Documents.RetiredAttachmentsStorage;

namespace Raven.Server.Documents
{
    public sealed class RetireAttachmentsSender : BackgroundWorkBase
    {
        public const int DefaultRetireFrequencyInSec = 60;
        public static int ReadTransactionMaxOpenTimeInMs = 60_000;
        internal static long BatchSizeInBytes = PlatformDetails.Is32Bits == false ? 1024 * Constants.Size.Megabyte : 4 * Constants.Size.Megabyte;
        internal static Size BatchSizeUnit = new Size(BatchSizeInBytes, SizeUnit.Bytes);
        internal static int BatchSize = PlatformDetails.Is32Bits == false ? 36 : 8;
        internal static short ConcurrentThreadsNumber = PlatformDetails.Is32Bits == false ? (short)8 : (short)2;

        private readonly DocumentDatabase _database;
        private readonly TimeSpan _retirePeriod;
        private  UploaderSettings _uploaderSettings;
        private readonly OperationCancelToken _token;
        private bool _allHalted =>  Configuration == null || Configuration.Destinations.Count == 0 || Configuration.Destinations.All(x => x.Value.Disabled == true);

        public RetiredAttachmentsConfiguration Configuration { get; }

        internal RetireAttachmentsSender(DocumentDatabase database, RetiredAttachmentsConfiguration retiredAttachmentsConfiguration) : base(database.Name, database.Loggers.GetLogger<RetireAttachmentsSender>(), database.DatabaseShutdown)
        {
            Configuration = retiredAttachmentsConfiguration;
            _database = database;
            _retirePeriod = TimeSpan.FromSeconds(Configuration?.RetireFrequencyInSec ?? DefaultRetireFrequencyInSec);
            _token = new OperationCancelToken(Cts.Token);
        }

        protected override Task DoWork()
        {
            if (_allHalted)
                return Task.CompletedTask;

            //TODO: egor now I got multiple uploaders :) need to handle that and not use first :)
            _uploaderSettings = UploaderSettings.GenerateDirectUploaderSetting(_database, nameof(RetireAttachmentsSender),
                Configuration.Destinations.First().Value.S3Settings, Configuration.Destinations.First().Value.AzureSettings, glacierSettings: null, googleCloudSettings: null, ftpSettings: null, ConcurrentThreadsNumber);

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
            var totalUploaded = 0L;

            var currentTime = _database.Time.GetUtcNow();
            try
            {
                var directUpload = new DirectFileUploader(_uploaderSettings, retentionPolicyParameters: null, Logger, FileUploaderBase.GenerateUploadResult(), onProgress: ProgressNotification, _token);
                using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                while (totalCount < maxItemsToProcess)
                {
                    context.Reset();
                    context.Renew();

                    Stopwatch duration;
                    var retired = new Queue<DocumentExpirationInfo>();

                    using (var tx = context.OpenReadTransaction())
                    using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(context))
                    {
                        DatabaseRecord dbRecord;
                        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                        using (serverContext.OpenReadTransaction())
                        {
                            dbRecord = _database.ServerStore.Cluster.ReadDatabase(serverContext, _database.Name);
                        }

                        var options = new BackgroundWorkParameters(context, currentTime, dbRecord, _database.ServerStore.NodeTag, batchSize, maxItemsToProcess);

                        Queue<DocumentExpirationInfo> toRetire =
                            _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out duration,
                                CancellationToken);

                        if (toRetire == null || toRetire.Count == 0)
                        {
                            return totalCount;
                        }

                        using (directUpload.Initialize())
                        {
                            // upload the attachments to cloud and update the document
                            foreach (var doc in toRetire)
                            {
                                _token.ThrowIfCancellationRequested();

                                var key = doc.LowerId.ToString();
                                var collection = doc.Id;

                                if (CanContinueBatch(Logger, duration, totalUploaded) == false)
                                {
                                    break;
                                }

                                if (string.IsNullOrEmpty(collection))
                                {
                                    if (Logger.IsInfoEnabled)
                                        Logger.Info($"Skipping '{nameof(AttachmentRetireType.PutRetire)}' of retired attachment with key: '{key}' because it's collection '{collection}' IsNullOrEmpty.");

                                    // document was deleted, need to remove it from retired tree
                                    retired.Enqueue(doc);
                                    continue;
                                }

                                var hash = _database.DocumentsStorage.AttachmentsStorage.ExtractHashFromAttachmentId(doc.LowerId);

                                if (directUpload.GetObjectMetadata(string.Empty, hash) != null)
                                {
                                    // the attachment already exists in the cloud, no need to upload it again
                                    retired.Enqueue(doc);
                                    continue;
                                }

                                // the attachment stream is disposed by the directUpload
                                var attachmentStream = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStreamByKey(context, doc.LowerId);
                                if (attachmentStream == null)
                                {
                                    // attachment was deleted, need to remote it from retired tree
                                    retired.Enqueue(doc);
                                    continue;
                                }

                                if (directUpload.TryCleanFinishedThreads(duration, _token))
                                {
                                    directUpload.CreateUploadTask(_database, attachmentStream, hash, CancellationToken);

                                    totalUploaded += attachmentStream.Length;
                                    retired.Enqueue(doc);
                                }
                                else
                                {
                                    LogTimeoutIfNeeded(nameof(AttachmentRetireType.PutRetire), key, duration);
                                }
                            }

                            if (toRetire.Count != retired.Count)
                            {
                                // we had skipped items
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"Skipping retiring of '{toRetire.Count - retired.Count:#,#;;0}' attachments, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => retired.Contains(x) == false))}");
                            }

                            if (retired.Count == 0)
                            {
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"Skipping retiring whole batch of '{retired.Count:#,#;;0}' attachments, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => retired.Contains(x) == false))}");

                                continue;
                            }
                        }
                    }

                    var command = new UpdateRetiredAttachmentsCommand(retired, _database, currentTime);
                    await _database.TxMerger.Enqueue(command);

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Successfully retired '{command.RetiredCount:#,#;;0}' attachments in '{duration.ElapsedMilliseconds:#,#;;0}' ms.");
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to retire attachments on '{_database.Name}' which are older than '{currentTime}'.", e);
            }
            return totalCount;
        }

        private static bool CanContinueBatch(RavenLogger logger, Stopwatch duration, long totalUploaded)
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

            return true;
        }

        private void LogTimeoutIfNeeded(string method, string key, Stopwatch sp)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Timed out waiting for free thread to {method} retired attachments with '{key}', ReadTransactionMaxOpenTimeInMs: {sp.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs}, IsCancellationRequested: {_token.Token.IsCancellationRequested}, the {method} will happen on next iteration.");
        }

        private void ProgressNotification(IOperationProgress progress)
        {

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
                RetiredCount = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.ProcessDocuments(context, _retired,   _currentTime);

                return RetiredCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new UpdateRetiredAttachmentsCommandDto
                {
                    Retired = _retired.Select(x => (Ticks: x.Ticks, LowerId: x.LowerId, Id: x.Id)).ToArray(),
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
            var retired = new Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo>();
            foreach (var item in Retired)
            {
                retired.Enqueue(new AbstractBackgroundWorkStorage.DocumentExpirationInfo(item.Item1.Clone(context.Allocator), item.Item2.Clone(context.Allocator), item.Item3));
            }
            var command = new RetireAttachmentsSender.UpdateRetiredAttachmentsCommand(retired, database, CurrentTime);
            return command;
        }

        public (Slice, Slice, string)[] Retired { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
