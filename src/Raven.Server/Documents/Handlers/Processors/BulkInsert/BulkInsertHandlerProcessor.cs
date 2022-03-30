using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Handlers.BulkInsert;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.BulkInsert;

internal class BulkInsertHandlerProcessor: AbstractBulkInsertHandlerProcessor<BatchRequestParser.CommandData, BulkInsertHandler, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;
    private readonly Logger _logger;
    private int _databaseChangeVectorSize;

    public BulkInsertHandlerProcessor([NotNull] BulkInsertHandler requestHandler, [NotNull] JsonContextPoolBase<DocumentsOperationContext> contextPool,
        DocumentDatabase database, Action<IOperationProgress> onProgress, bool skipOverwriteIfUnchanged, CancellationToken token) 
        : base(requestHandler, contextPool, onProgress, skipOverwriteIfUnchanged, token)
    {
        _database = database;
        _logger = LoggingSource.Instance.GetLogger<MergedInsertBulkCommand>(database.Name);
        _databaseChangeVectorSize = GetDatabaseChangeVectorSize();
    }

    protected override AbstractBulkInsertBatchCommandsReader<BatchRequestParser.CommandData> GetCommandsReader(JsonOperationContext context, Stream requestBodyStream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
    {
        return new BulkInsertBatchCommandsReader(context, requestBodyStream, buffer, token);
    }

    protected override async ValueTask ExecuteCommands(Task currentTask, int numberOfCommands, BatchRequestParser.CommandData[] array, long totalSize)
    {
        using (ReplaceContextIfCurrentlyInUse(currentTask, numberOfCommands, array))
        {
            await _database.TxMerger.Enqueue(new MergedInsertBulkCommand
            {
                Commands = array,
                NumberOfCommands = numberOfCommands,
                Database = _database,
                Logger = _logger,
                TotalSize = totalSize,
                SkipOverwriteIfUnchanged = SkipOverwriteIfUnchanged
            });
        }
    }

    protected override ValueTask<MergedBatchCommand.AttachmentStream> StoreAttachmentStream(BatchRequestParser.CommandData command, AbstractBulkInsertBatchCommandsReader<BatchRequestParser.CommandData> reader)
    {
        return WriteAttachment(command.ContentLength, reader.GetBlob(command.ContentLength));
    }

    protected override (long, int) GetSizeAndOperationsCount(BatchRequestParser.CommandData commandData)
    {
        return GetSizeAndOperationsCount(commandData, estimatedChangeVectorSize: _databaseChangeVectorSize);
    }
    private int GetDatabaseChangeVectorSize()
    {
        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
            return _databaseChangeVectorSize = Encoding.UTF8.GetBytes(databaseChangeVector).Length;
        }
    }

    private IDisposable ReplaceContextIfCurrentlyInUse(Task task, int numberOfCommands, BatchRequestParser.CommandData[] array)
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

    private async ValueTask<MergedBatchCommand.AttachmentStream> WriteAttachment(long size, Stream stream)
    {
        var attachmentStream = new MergedBatchCommand.AttachmentStream();

        if (size <= 32 * 1024)
        {
            attachmentStream.Stream = new MemoryStream();
        }
        else
        {
            StreamsTempFile attachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("bulk");
            attachmentStream.Stream = attachmentStreamsTempFile.StartNewStream();

            if (_streamsTempFiles == null)
                _streamsTempFiles = new List<StreamsTempFile>();

            _streamsTempFiles.Add(attachmentStreamsTempFile);
        }

        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenWriteTransaction())
        {
            attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(ctx, stream, attachmentStream.Stream, _database.DatabaseShutdown);
            await attachmentStream.Stream.FlushAsync();
        }

        return attachmentStream;
    }
}
