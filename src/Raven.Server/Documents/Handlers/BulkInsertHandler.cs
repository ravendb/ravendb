using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Exceptions;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateOperationToken();
            var id = GetLongQueryString("id");

            await Database.Operations.AddOperation(Database, "Bulk Insert", Operations.Operations.OperationType.BulkInsert,
                progress => DoBulkInsert(progress, operationCancelToken.Token),
                id,
                token: operationCancelToken
            );
        }

        private async Task<IOperationResult> DoBulkInsert(Action<IOperationProgress> onProgress, CancellationToken token)
        {
            var progress = new BulkInsertProgress();
            try
            {
                var logger = LoggingSource.Instance.GetLogger<MergedInsertBulkCommand>(Database.Name);
                IDisposable currentCtxReset = null, previousCtxReset = null;

                try
                {
                    using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (context.GetMemoryBuffer(out var buffer))
                    {
                        currentCtxReset = ContextPool.AllocateOperationContext(out JsonOperationContext docsCtx);
                        var requestBodyStream = RequestBodyStream();

                        using (var parser = new BatchRequestParser.ReadMany(context, requestBodyStream, buffer, token))
                        {
                            await parser.Init();

                            var array = new BatchRequestParser.CommandData[8];
                            var numberOfCommands = 0;
                            long totalSize = 0;
                            int operationsCount = 0;

                            while (true)
                            {
                                using (var modifier = new BlittableMetadataModifier(docsCtx))
                                {
                                    var task = parser.MoveNext(docsCtx, modifier);
                                    if (task == null)
                                        break;

                                    token.ThrowIfCancellationRequested();

                                    // if we are going to wait on the network, flush immediately
                                    if ((task.Wait(5) == false && numberOfCommands > 0) ||
                                        // but don't batch too much anyway
                                        totalSize > 16 * Voron.Global.Constants.Size.Megabyte || operationsCount >= 8192)
                                    {
                                        using (ReplaceContextIfCurrentlyInUse(task, numberOfCommands, array))
                                        {
                                            await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                            {
                                                Commands = array,
                                                NumberOfCommands = numberOfCommands,
                                                Database = Database,
                                                Logger = logger,
                                                TotalSize = totalSize
                                            });
                                        }

                                        ClearStreamsTempFiles();

                                        progress.BatchCount++;
                                        progress.Total += numberOfCommands;
                                        progress.LastProcessedId = array[numberOfCommands - 1].Id;

                                        onProgress(progress);

                                        previousCtxReset?.Dispose();
                                        previousCtxReset = currentCtxReset;
                                        currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);

                                        numberOfCommands = 0;
                                        totalSize = 0;
                                        operationsCount = 0;
                                    }

                                    var commandData = await task;
                                    if (commandData.Type == CommandType.None)
                                        break;

                                    if (commandData.Type == CommandType.AttachmentPUT)
                                    {
                                        commandData.AttachmentStream = await WriteAttachment(commandData.ContentLength, parser.GetBlob(commandData.ContentLength));
                                    }

                                    (long size, int opsCount) = GetSizeAndOperationsCount(commandData);
                                    operationsCount += opsCount;
                                    totalSize += size;
                                    if (numberOfCommands >= array.Length)
                                        Array.Resize(ref array, array.Length + Math.Min(1024, array.Length));
                                    array[numberOfCommands++] = commandData;

                                    switch (commandData.Type)
                                    {
                                        case CommandType.PUT:
                                            progress.DocumentsProcessed++;
                                            break;

                                        case CommandType.AttachmentPUT:
                                            progress.AttachmentsProcessed++;
                                            break;

                                        case CommandType.Counters:
                                            progress.CountersProcessed++;
                                            break;

                                        case CommandType.TimeSeriesBulkInsert:
                                            progress.TimeSeriesProcessed++;
                                            break;
                                    }
                                }
                            }

                            if (numberOfCommands > 0)
                            {
                                await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                {
                                    Commands = array,
                                    NumberOfCommands = numberOfCommands,
                                    Database = Database,
                                    Logger = logger,
                                    TotalSize = totalSize
                                });

                                progress.BatchCount++;
                                progress.Total += numberOfCommands;
                                progress.LastProcessedId = array[numberOfCommands - 1].Id;
#pragma warning disable CS0618 // Type or member is obsolete
                                progress.Processed = progress.DocumentsProcessed;
#pragma warning restore CS0618 // Type or member is obsolete

                                onProgress(progress);
                            }
                        }
                    }
                }
                finally
                {
                    currentCtxReset?.Dispose();
                    previousCtxReset?.Dispose();
                    ClearStreamsTempFiles();
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                return new BulkOperationResult
                {
                    Total = progress.Total,
                    DocumentsProcessed = progress.DocumentsProcessed,
                    AttachmentsProcessed = progress.AttachmentsProcessed,
                    CountersProcessed = progress.CountersProcessed,
                    TimeSeriesProcessed = progress.TimeSeriesProcessed
                };
            }
            catch (Exception e)
            {
                HttpContext.Response.Headers["Connection"] = "close";
                throw new InvalidOperationException("Failed to process bulk insert. " + progress, e);
            }
        }

        private void ClearStreamsTempFiles()
        {
            if (_streamsTempFiles == null)
                return;

            foreach (var file in _streamsTempFiles)
            {
                file.Dispose();
            }

            _streamsTempFiles = null;
        }

        private List<StreamsTempFile> _streamsTempFiles;

        private async Task<BatchHandler.MergedBatchCommand.AttachmentStream> WriteAttachment(long size, Stream stream)
        {
            var attachmentStream = new BatchHandler.MergedBatchCommand.AttachmentStream();

            if (size <= 32 * 1024)
            {
                attachmentStream.Stream = new MemoryStream();
            }
            else
            {
                StreamsTempFile attachmentStreamsTempFile = Database.DocumentsStorage.AttachmentsStorage.GetTempFile("bulk");
                attachmentStream.Stream = attachmentStreamsTempFile.StartNewStream();

                if (_streamsTempFiles == null)
                    _streamsTempFiles = new List<StreamsTempFile>();

                _streamsTempFiles.Add(attachmentStreamsTempFile);
            }

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenWriteTransaction())
            {
                attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(ctx, stream, attachmentStream.Stream, Database.DatabaseShutdown);
                await attachmentStream.Stream.FlushAsync();
            }

            return attachmentStream;
        }

        private int? _changeVectorSize;

        private (long, int) GetSizeAndOperationsCount(BatchRequestParser.CommandData commandData)
        {
            long size = 0;
            switch (commandData.Type)
            {
                case CommandType.PUT:
                    return (commandData.Document.Size, 1);

                case CommandType.Counters:
                    foreach (var operation in commandData.Counters.Operations)
                    {
                        size += operation.CounterName.Length
                                + sizeof(long) // etag
                                + sizeof(long) // counter value
                                + GetChangeVectorSizeInternal() // estimated change vector size
                                + 10; // estimated collection name size
                    }

                    return (size, commandData.Counters.Operations.Count);

                case CommandType.AttachmentPUT:
                    return (commandData.ContentLength, 1);

                case CommandType.TimeSeries:
                case CommandType.TimeSeriesBulkInsert:
                    // we don't know the size of the change so we are just estimating
                    foreach (var append in commandData.TimeSeries.Appends)
                    {
                        size += sizeof(long); // DateTime
                        if (string.IsNullOrWhiteSpace(append.Tag) == false)
                            size += append.Tag.Length;

                        size += append.Values.Length * sizeof(double);
                    }

                    return (size, commandData.TimeSeries.Appends.Count);

                default:
                    throw new ArgumentOutOfRangeException($"'{commandData.Type}' isn't supported");
            }

            int GetChangeVectorSizeInternal()
            {
                if (_changeVectorSize.HasValue)
                    return _changeVectorSize.Value;

                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                    _changeVectorSize = Encoding.UTF8.GetBytes(databaseChangeVector).Length;
                    return _changeVectorSize.Value;
                }
            }
        }

        private IDisposable ReplaceContextIfCurrentlyInUse(Task<BatchRequestParser.CommandData> task, int numberOfCommands, BatchRequestParser.CommandData[] array)
        {
            if (task.IsCompleted)
                return null;

            var disposable = ContextPool.AllocateOperationContext(out JsonOperationContext tempCtx);
            // the docsCtx is currently in use, so we
            // cannot pass it to the tx merger, we'll just
            // copy the documents to a temporary ctx and
            // use that ctx instead. Copying the documents
            // is safe, because they are immutables

            for (int i = 0; i < numberOfCommands; i++)
            {
                if (array[i].Document != null)
                {
                    array[i].Document = array[i].Document.Clone(tempCtx);
                }
            }
            return disposable;
        }

        public class MergedInsertBulkCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public Logger Logger;
            public DocumentDatabase Database;
            public BatchRequestParser.CommandData[] Commands;
            public int NumberOfCommands;
            public long TotalSize;

            private readonly Dictionary<string, DocumentUpdates> _documentsToUpdate = new Dictionary<string, DocumentUpdates>(StringComparer.OrdinalIgnoreCase);

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                for (int i = 0; i < NumberOfCommands; i++)
                {
                    var cmd = Commands[i];

                    Debug.Assert(cmd.Type == CommandType.PUT || cmd.Type == CommandType.Counters || cmd.Type == CommandType.TimeSeries || cmd.Type == CommandType.TimeSeriesBulkInsert || cmd.Type == CommandType.AttachmentPUT);

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:
                            try
                            {
                                Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document);
                            }
                            catch (VoronConcurrencyErrorException)
                            {
                                // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                                // this means that we have existing values under the current etag
                                // we'll generate a new (random) id for them.

                                // The TransactionMerger will re-run us when we ask it to as a
                                // separate transaction

                                for (; i < NumberOfCommands; i++)
                                {
                                    cmd = Commands[i];
                                    if (cmd.Type != CommandType.PUT)
                                        continue;

                                    if (cmd.Id?.EndsWith(Database.IdentityPartsSeparator) == true)
                                    {
                                        cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                        RetryOnError = true;
                                    }
                                }

                                throw;
                            }

                            break;

                        case CommandType.Counters:
                            {
                                var collection = CountersHandler.ExecuteCounterBatchCommand.GetDocumentCollection(cmd.Id, Database, context, fromEtl: false, out _);

                                foreach (var counterOperation in cmd.Counters.Operations)
                                {
                                    counterOperation.DocumentId = cmd.Counters.DocumentId;
                                    Database.DocumentsStorage.CountersStorage.IncrementCounter(context, cmd.Id, collection, counterOperation.CounterName, counterOperation.Delta, out _);

                                    var updates = GetDocumentUpdates(cmd.Id);
                                    updates.AddCounter(counterOperation.CounterName);
                                }

                                break;
                            }
                        case CommandType.TimeSeries:
                        case CommandType.TimeSeriesBulkInsert:
                            {
                                var docCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.Id, fromEtl: false);
                                Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                                    cmd.Id,
                                    docCollection,
                                    cmd.TimeSeries.Name,
                                    cmd.TimeSeries.Appends
                                );
                                break;
                            }
                        case CommandType.AttachmentPUT:
                            {
                                using (cmd.AttachmentStream.Stream)
                                {
                                    Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, cmd.Id, cmd.Name,
                                        cmd.ContentType ?? "", cmd.AttachmentStream.Hash, cmd.ChangeVector, cmd.AttachmentStream.Stream, updateDocument: false);
                                }

                                var updates = GetDocumentUpdates(cmd.Id);
                                updates.AddAttachment();

                                break;
                            }
                    }
                }

                if (_documentsToUpdate.Count > 0)
                {
                    foreach (var kvp in _documentsToUpdate)
                    {
                        var documentId = kvp.Key;
                        var updates = kvp.Value;

                        if (updates.Attachments)
                            Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId);

                        if (updates.Counters != null && updates.Counters.Count > 0)
                        {
                            var docToUpdate = Database.DocumentsStorage.Get(context, documentId);
                            if (docToUpdate != null)
                            {
                                Database.DocumentsStorage.CountersStorage.UpdateDocumentCounters(context, docToUpdate, documentId, updates.Counters, countersToRemove: null, NonPersistentDocumentFlags.ByCountersUpdate);
                            }
                        }
                    }
                }

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Executed {NumberOfCommands:#,#;;0} bulk insert operations, size: ({new Size(TotalSize, SizeUnit.Bytes)})");
                }

                return NumberOfCommands;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedInsertBulkCommandDto
                {
                    Commands = Commands.Take(NumberOfCommands).ToArray()
                };
            }

            private DocumentUpdates GetDocumentUpdates(string documentId)
            {
                if (_documentsToUpdate.TryGetValue(documentId, out var update) == false)
                    _documentsToUpdate[documentId] = update = new DocumentUpdates();

                return update;
            }

            private class DocumentUpdates
            {
                public bool Attachments;

                public SortedSet<string> Counters;

                public void AddCounter(string counterName)
                {
                    if (Counters == null)
                        Counters = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                    Counters.Add(counterName);
                }

                public void AddAttachment()
                {
                    Attachments = true;
                }
            }
        }
    }

    public class MergedInsertBulkCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BulkInsertHandler.MergedInsertBulkCommand>
    {
        public BatchRequestParser.CommandData[] Commands { get; set; }

        public BulkInsertHandler.MergedInsertBulkCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BulkInsertHandler.MergedInsertBulkCommand
            {
                NumberOfCommands = Commands.Length,
                TotalSize = Commands.Sum(c => c.Document.Size),
                Commands = Commands,
                Database = database,
                Logger = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name)
            };
        }
    }
}
