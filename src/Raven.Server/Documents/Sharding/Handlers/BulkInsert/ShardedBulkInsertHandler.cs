using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.BulkInsert;
using Raven.Server.Documents.Sharding.Operations.BulkInsert;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/bulk_insert", "POST")]
    public async Task BulkInsert()
    {
        var id = GetLongQueryString("id");
        var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

        await DoBulkInsert(x =>
        {

        }, id, skipOverwriteIfUnchanged, CancellationToken.None);
    }

    private async Task<IOperationResult> DoBulkInsert(Action<IOperationProgress> onProgress, long id, bool skipOverwriteIfUnchanged, CancellationToken token)
    {
        var progress = new BulkInsertProgress();
        try
        {
            var logger = LoggingSource.Instance.GetLogger<BulkInsertHandler.MergedInsertBulkCommand>(DatabaseContext.DatabaseName);

            IDisposable currentCtxReset = null, previousCtxReset = null;

            try
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                await using (var operation = new ShardedBulkInsertOperation(id, skipOverwriteIfUnchanged, DatabaseContext, context))
                {
                    var requestBodyStream = RequestBodyStream();

                    if (ClientSentGzipRequest())
                    {
                        operation.CompressionLevel = CompressionLevel.Optimal;
                    }

                    currentCtxReset = ContextPool.AllocateOperationContext(out JsonOperationContext docsCtx);

                    using (var reader = new ShardedBulkInsertCommandsReader(context, requestBodyStream, buffer, token))
                    {
                        await reader.Init();

                        var array = new ShardedCommandData[8];
                        var numberOfCommands = 0;
                        long totalSize = 0;
                        int operationsCount = 0;

                        while (true)
                        {
                            using (var modifier = new BlittableMetadataModifier(docsCtx))
                            {
                                var task = reader.GetCommandAsync(docsCtx, modifier);
                                if (task == null)
                                    break;

                                token.ThrowIfCancellationRequested();

                                // if we are going to wait on the network, flush immediately
                                if ((task.Wait(5) == false && numberOfCommands > 0) ||
                                    // but don't batch too much anyway
                                    totalSize > 16 * Voron.Global.Constants.Size.Megabyte || operationsCount >= 8192)
                                {
                                    //using (ReplaceContextIfCurrentlyInUse(task, numberOfCommands, array))
                                    {
                                        foreach (var readCommand in array)
                                        {
                                            if (readCommand is null)
                                                break;

                                            using (readCommand)
                                            {
                                                await operation.StoreAsync(readCommand.Stream, readCommand.Data.Id);
                                            }
                                        }

                                        // TODO arek
                                        //await Database.TxMerger.Enqueue(new BulkInsertHandler.MergedInsertBulkCommand
                                        //{
                                        //    Commands = array,
                                        //    NumberOfCommands = numberOfCommands,
                                        //    Database = Database,
                                        //    Logger = logger,
                                        //    TotalSize = totalSize,
                                        //    SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged
                                        //});
                                    }

                                    //ClearStreamsTempFiles();

                                    progress.BatchCount++;
                                    progress.Total += numberOfCommands;
                                    progress.LastProcessedId = array[numberOfCommands - 1].Data.Id;

                                    onProgress(progress);

                                    previousCtxReset?.Dispose();
                                    previousCtxReset = currentCtxReset;
                                    currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);

                                    numberOfCommands = 0;
                                    totalSize = 0;
                                    operationsCount = 0;
                                }

                                var command = await task;

                                if (command.Data.Type == CommandType.None)
                                    break;

                                if (command.Data.Type == CommandType.AttachmentPUT)
                                {
                                    throw new NotImplementedException("TODO arek");
                                    //command.Data.AttachmentStream = await WriteAttachment(command.ContentLength, reader.GetBlob(command.ContentLength));
                                }

                                (long size, int opsCount) = GetSizeAndOperationsCount(command.Data);
                                operationsCount += opsCount;
                                totalSize += size;
                                if (numberOfCommands >= array.Length)
                                    Array.Resize(ref array, array.Length + Math.Min(1024, array.Length));
                                array[numberOfCommands++] = command;

                                switch (command.Data.Type)
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
                            //await MetricCacher.Keys.Database.TxMerger.Enqueue(new BulkInsertHandler.MergedInsertBulkCommand
                            //{
                            //    Commands = array,
                            //    NumberOfCommands = numberOfCommands,
                            //    Database = MetricCacher.Keys.Database,
                            //    Logger = logger,
                            //    TotalSize = totalSize,
                            //    SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged
                            //});

                            foreach (var command in array)
                            {
                                if (command is null)
                                    break;

                                using (command)
                                {
                                    await operation.StoreAsync(command.Stream, command.Data.Id);
                                }
                            }

                            progress.BatchCount++;
                            progress.Total += numberOfCommands;
                            progress.LastProcessedId = array[numberOfCommands - 1].Data.Id;
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
                //ClearStreamsTempFiles();
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
            return 4; // TODO arek
            //if (_changeVectorSize.HasValue)
            //    return _changeVectorSize.Value;

            //using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            //using (ctx.OpenReadTransaction())
            //{
            //    var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
            //    _changeVectorSize = Encoding.UTF8.GetBytes(databaseChangeVector).Length;
            //    return _changeVectorSize.Value;
            //}
        }
    }

    private int? _changeVectorSize;


    //private IDisposable ReplaceContextIfCurrentlyInUse(Task<BatchRequestParser.CommandData> task, int numberOfCommands, BatchRequestParser.CommandData[] array)
    //{
    //    if (task.IsCompleted)
    //        return null;

    //    var disposable = ContextPool.AllocateOperationContext(out JsonOperationContext tempCtx);
    //    // the docsCtx is currently in use, so we
    //    // cannot pass it to the tx merger, we'll just
    //    // copy the documents to a temporary ctx and
    //    // use that ctx instead. Copying the documents
    //    // is safe, because they are immutables

    //    for (int i = 0; i < numberOfCommands; i++)
    //    {
    //        if (array[i].Document != null)
    //        {
    //            array[i].Document = array[i].Document.Clone(tempCtx);
    //        }
    //    }
    //    return disposable;
    //}
}
