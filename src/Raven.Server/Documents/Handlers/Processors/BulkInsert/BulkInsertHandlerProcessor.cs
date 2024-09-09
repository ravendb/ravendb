using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.BulkInsert;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.BulkInsert;

internal sealed class BulkInsertHandlerProcessor: AbstractBulkInsertHandlerProcessor<BatchRequestParser.CommandData, BulkInsertHandler, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;
    private readonly Logger _logger;
    private int _databaseChangeVectorSize;

    public BulkInsertHandlerProcessor([NotNull] BulkInsertHandler requestHandler,
        DocumentDatabase database, Action<IOperationProgress> onProgress, bool skipOverwriteIfUnchanged, CancellationToken token) 
        : base(requestHandler, onProgress, skipOverwriteIfUnchanged, token)
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

    protected override StreamsTempFile GetTempFile()
    {
        return _database.DocumentsStorage.AttachmentsStorage.GetTempFile("bulk-insert");
    }

    protected override async ValueTask<string> CopyAttachmentStreamAsync(Stream sourceStream, Stream attachmentStream, CancellationToken token)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(ctx, sourceStream, attachmentStream, token);
            return hash;
        }
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
}
