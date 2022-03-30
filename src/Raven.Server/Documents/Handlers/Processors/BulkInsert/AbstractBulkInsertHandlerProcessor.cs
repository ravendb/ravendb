using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Web;
using Sparrow.Json;
using Raven.Server.Documents.Handlers.BulkInsert;

namespace Raven.Server.Documents.Handlers.Processors.BulkInsert;

internal abstract class AbstractBulkInsertHandlerProcessor<TCommandData, TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TCommandData : IBatchCommandData
    where TOperationContext : JsonOperationContext
{
    private readonly CancellationToken _token;
    private readonly BulkInsertProgress _progress;

    private readonly Action<IOperationProgress> _onProgress;
    protected readonly bool SkipOverwriteIfUnchanged;
    protected List<StreamsTempFile> _streamsTempFiles; // TODO arek

    protected AbstractBulkInsertHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool, Action<IOperationProgress> onProgress, bool skipOverwriteIfUnchanged, CancellationToken token)
        : base(requestHandler, contextPool)
    {
        _onProgress = onProgress;
        SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
        _token = token;
        _progress = new BulkInsertProgress();
    }

    protected abstract AbstractBulkInsertBatchCommandsReader<TCommandData> GetCommandsReader(JsonOperationContext context, Stream requestBodyStream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token);

    protected abstract ValueTask ExecuteCommands(Task currentTask, int numberOfCommands, TCommandData[] array, long totalSize);

    protected abstract ValueTask<MergedBatchCommand.AttachmentStream> StoreAttachmentStream(TCommandData command, AbstractBulkInsertBatchCommandsReader<TCommandData> abstractBulkInsertBatchCommandsReader);

    protected abstract (long, int) GetSizeAndOperationsCount(TCommandData commandData);

    public BulkOperationResult OperationResult { get; private set; }

    public override async ValueTask ExecuteAsync()
    {
        try
        {
            IDisposable currentCtxReset = null, previousCtxReset = null;

            try
            {
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                {
                    currentCtxReset = ContextPool.AllocateOperationContext(out JsonOperationContext docsCtx);
                    var requestBodyStream = RequestHandler.RequestBodyStream();

                    using (var reader = GetCommandsReader(context, requestBodyStream, buffer, _token))
                    {
                        await reader.Init();

                        var array = new TCommandData[8];
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

                                _token.ThrowIfCancellationRequested();

                                // if we are going to wait on the network, flush immediately
                                if ((task.Wait(5) == false && numberOfCommands > 0) ||
                                    // but don't batch too much anyway
                                    totalSize > 16 * Voron.Global.Constants.Size.Megabyte || operationsCount >= 8192)
                                {
                                    await ExecuteCommands(task, numberOfCommands, array, totalSize);

                                    ClearStreamsTempFiles();

                                    _progress.BatchCount++;
                                    _progress.Total += numberOfCommands;
                                    _progress.LastProcessedId = array[numberOfCommands - 1].Id;

                                    _onProgress?.Invoke(_progress);

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
                                    commandData.AttachmentStream = await StoreAttachmentStream(commandData, reader); // TODO arek - IMPORTANT
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
                                        _progress.DocumentsProcessed++;
                                        break;

                                    case CommandType.AttachmentPUT:
                                        _progress.AttachmentsProcessed++;
                                        break;

                                    case CommandType.Counters:
                                        _progress.CountersProcessed++;
                                        break;

                                    case CommandType.TimeSeriesBulkInsert:
                                        _progress.TimeSeriesProcessed++;
                                        break;
                                }
                            }
                        }

                        if (numberOfCommands > 0)
                        {
                            await ExecuteCommands(Task.CompletedTask, numberOfCommands, array, totalSize);

                            _progress.BatchCount++;
                            _progress.Total += numberOfCommands;
                            _progress.LastProcessedId = array[numberOfCommands - 1].Id;
#pragma warning disable CS0618 // Type or member is obsolete
                            _progress.Processed = _progress.DocumentsProcessed;
#pragma warning restore CS0618 // Type or member is obsolete

                            _onProgress?.Invoke(_progress);
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

            OperationResult = new BulkOperationResult
            {
                Total = _progress.Total,
                DocumentsProcessed = _progress.DocumentsProcessed,
                AttachmentsProcessed = _progress.AttachmentsProcessed,
                CountersProcessed = _progress.CountersProcessed,
                TimeSeriesProcessed = _progress.TimeSeriesProcessed
            };
        }
        catch (Exception e)
        {
            HttpContext.Response.Headers["Connection"] = "close";

            throw new InvalidOperationException("Failed to process bulk insert. " + _progress, e);
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

    protected static (long, int) GetSizeAndOperationsCount(BatchRequestParser.CommandData commandData, long estimatedChangeVectorSize)
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
                            + estimatedChangeVectorSize // estimated change vector size
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
    }
}
