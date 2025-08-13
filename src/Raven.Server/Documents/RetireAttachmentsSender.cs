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
        private readonly OperationCancelToken _token;
        private bool _allHalted => Configuration == null || Configuration.Destinations.Count == 0 || Configuration.Destinations.All(x => x.Value.Disabled == true);

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
                using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                while (totalCount < maxItemsToProcess)
                {
                    context.Reset();
                    context.Renew();

                    Stopwatch duration;
                    var retired = new Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo>();

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

                        Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> toRetire =
                            _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out duration,
                                CancellationToken);

                        if (toRetire == null || toRetire.Count == 0)
                        {
                            return totalCount;
                        }

                        var directUploaders = new Dictionary<string, DirectFileUploader>();

                        try
                        {
                            // upload the attachments to cloud and update the document
                            foreach (var doc in toRetire)
                            {
                                _token.ThrowIfCancellationRequested();

                                if (CanContinueBatch(Logger, duration, totalUploaded) == false)
                                {
                                    break;
                                }

                                if (string.IsNullOrEmpty(doc.Id))
                                {
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug($"Skipping PutRetire of retired attachment with key: '{doc.LowerId}' because it's identifier '{doc.Id}' IsNullOrEmpty.");

                                    // document was deleted, need to remove it from retired tree
                                    retired.Enqueue(doc);
                                    continue;
                                }


                                // get uploader for the attachment
                                if (directUploaders.TryGetValue(doc.Id, out var directUpload) == false)
                                {
                                    // check if we have a destination configuration for the identifier

                                    if (Configuration.Destinations.TryGetValue(doc.Id, out var destination) == false)
                                    {
                                        // no destination found for the identifier
                                        if (Logger.IsWarnEnabled)
                                            Logger.Warn($"No destination found for retired attachment with key: '{doc.LowerId}' and identifier '{doc.Id}'. Will try to retire it again!");

                                        // this will keep the retired attachment in the tree and will try to retire it again afterwards.

                                        totalCount--; // let's decrease the total count in case we have a lot of attachments without destination that we skip
                                        continue;
                                    }

                                    if (destination.HasUploader() == false)
                                    {
                                        // no uploader for this destination
                                        if (Logger.IsWarnEnabled)
                                            Logger.Warn($"No uploader found for destination with identifier '{destination.Identifier}' for attachment with key: '{doc.LowerId}'. Will try to retire it again!");

                                        totalCount--;
                                        continue;
                                    }

                                    // create new uploader for the attachment
                                    directUploaders[doc.Id] = directUpload = new DirectFileUploader(UploaderSettings.GenerateDirectUploaderSetting(_database, nameof(RetireAttachmentsSender),
                                            destination.S3Settings, destination.AzureSettings, glacierSettings: null, googleCloudSettings: null, ftpSettings: null, ConcurrentThreadsNumber), retentionPolicyParameters: null, Logger,
                                        FileUploaderBase.GenerateUploadResult(), onProgress: ProgressNotification, _token);
                                }

                                using var hashSliceDisposable = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.ExtractHashSliceFromAttachmentId(context, doc.LowerId, out Slice hashSlice);
                                var hash = hashSlice.ToString();

                                if (directUpload.GetObjectMetadata(string.Empty, hash) != null)
                                {
                                    // the attachment already exists in the cloud, no need to upload it again
                                    retired.Enqueue(doc);
                                    continue;
                                }

                                // the attachment stream is disposed by the directUpload
                                var attachmentStream = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, hashSlice);
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
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug($"Timed out waiting for free thread to PutRetire retired attachments with '{doc.LowerId}', ReadTransactionMaxOpenTimeInMs: {duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs}, IsCancellationRequested: {_token.Token.IsCancellationRequested}, the PutRetire will happen on next iteration.");
                                }
                            }

                            if (toRetire.Count != retired.Count)
                            {
                                // we had skipped items
                                if (Logger.IsDebugEnabled)
                                    Logger.Debug($"Skipping retiring of '{toRetire.Count - retired.Count:#,#;;0}' attachments, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => retired.Contains(x) == false))}");
                            }

                            if (retired.Count == 0)
                            {
                                if (Logger.IsDebugEnabled)
                                    Logger.Debug($"Skipping retiring whole batch of '{retired.Count:#,#;;0}' attachments, Uploaded: {new Size(totalUploaded, SizeUnit.Bytes)}, read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => retired.Contains(x) == false))}");

                                continue;
                            }
                        }
                        finally
                        {
                            // Wait for all direct uploaders to finish
                            foreach (var uploader in directUploaders.Values)
                            {
                                uploader.Reset();
                            }
                        }
                    }

                    var command = new UpdateRetiredAttachmentsCommand(retired, _database, currentTime);
                    await _database.TxMerger.Enqueue(command);

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Successfully retired '{command.RetiredCount:#,#;;0}' attachments in '{duration.ElapsedMilliseconds:#,#;;0}' ms.");
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

        private void ProgressNotification(IOperationProgress progress)
        {

        }

        internal sealed class UpdateRetiredAttachmentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> _retired;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int RetiredCount;

            public UpdateRetiredAttachmentsCommand(Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> retired, DocumentDatabase database, DateTime currentTime)
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
            var retired = new Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo>();
            foreach (var item in Retired)
            {
                retired.Enqueue(new AbstractBackgroundWorkStorage.DocumentExpirationInfo(item.Item1.Clone(context.Allocator), item.Item2.Clone(context.Allocator), item.Item3, item.Item4));
            }
            var command = new RetireAttachmentsSender.UpdateRetiredAttachmentsCommand(retired, database, CurrentTime);
            return command;
        }

        public (Slice, Slice, string, AbstractBackgroundWorkStorage.DocumentExpirationInfoStatus)[] Retired { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
