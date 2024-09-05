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
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron;
using static Raven.Server.Documents.AbstractBackgroundWorkStorage;

namespace Raven.Server.Documents
{
    public sealed class RetireAttachmentsSender : BackgroundWorkBase
    {
        public const int DefaultRetireFrequencyInSec = 60;
        public static int ReadTransactionMaxOpenTimeInMs = 60_000;
        //TODO: egor actually make sure to limit the batch size
        internal static int BatchSizeInMb = PlatformDetails.Is32Bits == false
            ? 1024
            : 8;
        internal static int DefaultMaxItemsToProcessInSingleRun = int.MaxValue;

        private readonly DocumentDatabase _database;
        private readonly TimeSpan _retirePeriod;
        private  UploaderSettings _uploaderSettings;
        private readonly OperationCancelToken _token;
        private RetireAttachmentsStatsScope _uploadScope;

        public RetiredAttachmentsConfiguration Configuration { get; }

        private RetireAttachmentsSender(DocumentDatabase database, RetiredAttachmentsConfiguration retiredAttachmentsConfiguration) : base(database.Name, database.DatabaseShutdown)
        {
            Configuration = retiredAttachmentsConfiguration;
            _database = database;
            _retirePeriod = TimeSpan.FromSeconds(Configuration?.RetireFrequencyInSec ?? DefaultRetireFrequencyInSec);
            _token = new OperationCancelToken(Cts.Token);
        }

        protected override Task DoWork()
        {
            if (Configuration == null || Configuration.Disabled)
                return Task.CompletedTask;

            _uploaderSettings = UploaderSettings.GenerateDirectUploaderSetting(_database, nameof(RetireAttachmentsSender),
                Configuration.S3Settings, Configuration.AzureSettings, glacierSettings: null, googleCloudSettings: null, ftpSettings: null);

            var t = Task.Run(async () =>
            {
                //TODO: egor does it even can change ?
                while (Configuration.Disabled == false)
                {
                    await WaitOrThrowOperationCanceled(_retirePeriod);
                    await RetireAttachments(BatchSizeInMb, Configuration.MaxItemsToProcess ?? DefaultMaxItemsToProcessInSingleRun);
                }
            });
            return t;
        }

        internal async Task<int> RetireAttachments(int batchSize, long maxItemsToProcess)
        {
            if (Configuration.HasUploader() == false)
            {
                //Console.WriteLine($"Cannot retire attachments on '{_database.Name}' because no destination is configured.");
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Cannot retire attachments on '{_database.Name}' because no destination is configured.");
                return 0;
            }

            var totalCount = 0;

            var currentTime = _database.Time.GetUtcNow();
            //var myCounter = new List<string>();
            //var myCounter2 = new HashSet<string>();
            try
            {
                DatabaseRecord dbRecord;
                string nodeTag;
                //TODO: egor move this inside loop?
                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    dbRecord = _database.ServerStore.Cluster.ReadDatabase(serverContext, _database.Name);
                    nodeTag = _database.ServerStore.NodeTag;
                }

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (totalCount < maxItemsToProcess)
                    {
                        context.Reset();
                        context.Renew();

                        Stopwatch duration;
                        var retired = new Queue<DocumentExpirationInfo>();
                        using (context.OpenReadTransaction())
                        using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(context))
                        {
                            var options = new BackgroundWorkParameters(context, currentTime, dbRecord, nodeTag, batchSize);

                            Queue<DocumentExpirationInfo> toRetire = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out duration, CancellationToken);
                            if (toRetire == null || toRetire.Count == 0)
                            {
                                //Console.WriteLine("toRetire == null || toRetire.Count == 0");
                                return totalCount;
                            }
                            // TODO: egor this can be initialized once
                            // upload the attachments to cloud and update the document
                            using (DirectBackupUploader directUpload = new DirectBackupUploader(_uploaderSettings, retentionPolicyParameters: null, Logger, BackupUploaderBase.GenerateUploadResult(), onProgress: ProgressNotification, _token))
                            {
                                var shouldUpload = true;
                                var skippedIds = new HashSet<string>();
                                foreach (var doc in toRetire)
                                {
                                    _token.ThrowIfCancellationRequested();
                   
                                    var key = doc.LowerId;
                                    var collection = doc.Id;
                                    if (shouldUpload == false)
                                    {
                                        // we skip the uploads or deletes of retired attachments since the can take a long time
                                        skippedIds.Add(key.ToString());
                                        continue;
                                    }

                                    var type = _database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetRetireType(key);
                                    switch (type)
                                    {
                                        case RetiredAttachmentsStorage.AttachmentRetireType.PutRetire:
                                            if (string.IsNullOrEmpty(collection))
                                            {
                                                if (Logger.IsInfoEnabled)
                                                    Logger.Info($"Skipping 'PUT' of retired attachment with key: '{key.ToString()}' because it's collection IsNullOrEmpty.");

                                                // document was deleted, need to remove it from retired tree
                                                retired.Enqueue(doc);
                                                continue;
                                            }

                                            using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.CleanRetiredAttachmentsKey(options.Context, key, out var keySlice))
                                            await using (var attachmentStream = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStreamByKey(context, keySlice))
                                            {
                                                if (attachmentStream == null)
                                                {
                                                    // attachment was deleted, need to remote it from retired tree
                                                    retired.Enqueue(doc);
                                                    continue;
                                                }

                                                if (directUpload.TryCleanFinishedThreads(duration, _token))
                                                {
                                                    string objKeyName = GetBlobDestination(keySlice, collection, out string folderName);
                                                    directUpload.CreateUploadTask(_database, attachmentStream, folderName, objKeyName, CancellationToken);
                                                  
                                                    retired.Enqueue(doc);
                                                }
                                                else
                                                {
                                                    // threads got exceptions, token canceled or ReadTransactionMaxOpenTimeInMs is hit
                                                    _token.ThrowIfCancellationRequested();
                                                    LogTimeoutIfNeeded("put", key.ToString(), duration);
                                                    skippedIds.Add(key.ToString());
                                                }
                                            }

                                            if (duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs)
                                            {
                                                if (Logger.IsInfoEnabled)
                                                    Logger.Info($"Stop handling retired attachments to cloud due to long read tx open time: '{duration.ElapsedMilliseconds}'.");

                                                shouldUpload = false;
                                            }

                                            break;
                                        case RetiredAttachmentsStorage.AttachmentRetireType.DeleteRetire:
                                            if (string.IsNullOrEmpty(collection))
                                            {
                                                // configuration was changed, need to remove it from retired tree
                                                retired.Enqueue(doc);
                                                continue;
                                            }


                                            if (directUpload.TryCleanFinishedThreads(duration, _token))
                                            {
                                                using (_database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.CleanRetiredAttachmentsKey(options.Context, doc.LowerId, out var keySlice))
                                                {
                                                    string objKeyName = GetBlobDestination(keySlice, collection, out string folderName);
                                                    directUpload.AddDelete(folderName, objKeyName);
                                                }

                                                retired.Enqueue(doc);
                                            }
                                            else
                                            {
                                                _token.ThrowIfCancellationRequested();
                                                LogTimeoutIfNeeded("delete", key.ToString(), duration);
                                                skippedIds.Add(key.ToString());
                                            }

                                            if (duration.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs)
                                            {
                                                if (Logger.IsInfoEnabled)
                                                    Logger.Info($"Stop handling retired attachments to cloud due to long read tx open time: '{duration.ElapsedMilliseconds}'.");

                                                shouldUpload = false;
                                            }

                                            break;
                                        default:
                                            throw new ArgumentOutOfRangeException(nameof(type));
                                    }

                                }

                                if (skippedIds.Count > 0)
                                {
                                    if (Logger.IsInfoEnabled)
                                        Logger.Info($"Skipping retiring of '{skippedIds.Count:#,#;;0}' attachments, shouldUpload: '{shouldUpload}', read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", skippedIds)}");
                                }

                                if (retired.Count == 0)
                                {
                                    if (Logger.IsInfoEnabled)
                                        Logger.Info($"Skipping retiring whole batch of '{retired.Count:#,#;;0}' attachments, shouldUpload: '{shouldUpload}', read tx open time: '{duration.ElapsedMilliseconds}'. Skipped keys: {string.Join(", ", toRetire.Select(x => x.LowerId))}");

                                    continue;
                                }
                            }

                            //foreach (var x in retired)
                            //{
                            //    //Console.WriteLine($"retired: {x.LowerId}");
                            //}

                            //Console.WriteLine("---------");
                            //myCounter.AddRange(retired.Select(x=>x.LowerId.ToString()));
                            //foreach (string s in retired.Select(x => x.LowerId.ToString()))
                            //{
                            //    if (myCounter2.Add(s) == false)
                            //    {
                            //        Console.WriteLine($"Duplicate {s}");
                            //    }

                            //}
                            //Console.WriteLine($"Sending Command, totalCount: {totalCount} | retired.Count: {retired.Count} | myCounter: {myCounter2.Count}");
                        }

                        var command = new UpdateRetiredAttachmentsCommand(retired, _database, currentTime);
                            await _database.TxMerger.Enqueue(command);

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"Successfully retired '{command.RetiredCount:#,#;;0}' attachments in '{duration.ElapsedMilliseconds:#,#;;0}' ms.");
                    }
                }
            }
            catch (OperationCanceledException ee)
            {
                //Console.WriteLine($"RetireAttachmentsSender: " + ee);

                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                //Console.WriteLine($"RetireAttachmentsSender: "+e);
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to retire attachments on '{_database.Name}' which are older than '{currentTime}'.", e);
            }
            return totalCount;
        }

        private void LogTimeoutIfNeeded(string method, string key, Stopwatch sp)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Timed out waiting for free thread to {method} retired attachments with '{key}', ReadTransactionMaxOpenTimeInMs: {sp.ElapsedMilliseconds > ReadTransactionMaxOpenTimeInMs}, IsCancellationRequested: {_token.Token.IsCancellationRequested}, the {method} will happen on next iteration.");
        }

        private string GetBlobDestination(Slice keySlice, string collection, out string folderName)
        {
            var keyStr = keySlice.ToString();
            var objKeyName = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(keyStr));

            if (string.IsNullOrEmpty(collection))
            {
               throw new ArgumentException($"Collection is empty for key: {keyStr}");
            }

            //  folderName = $"{_database.Name}/{collection}";
            folderName = $"{collection}";
            return objKeyName;
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

        public static RetireAttachmentsSender LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, RetireAttachmentsSender retireAttachmentsSender)
        {
            try
            {
                if (dbRecord.RetiredAttachments == null)
                {
                    retireAttachmentsSender?.Dispose();
                    return null;
                }

                if (retireAttachmentsSender != null)
                {
                    // no changes
                    if (Equals(retireAttachmentsSender.Configuration, dbRecord.RetiredAttachments))
                        return retireAttachmentsSender;
                }

                retireAttachmentsSender?.Dispose();


                if (dbRecord.RetiredAttachments.Disabled)
                    return null;

                var cleaner = new RetireAttachmentsSender(database, dbRecord.RetiredAttachments);
                cleaner.Start();
                return cleaner;
            }
            catch (Exception e)
            {
                const string msg = $"Cannot enable {nameof(RetireAttachmentsSender)} as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"Expiration error in '{database.Name}'", msg,
                    AlertType.RetireAttachmentsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<RetireAttachmentsSender>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                return null;
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
