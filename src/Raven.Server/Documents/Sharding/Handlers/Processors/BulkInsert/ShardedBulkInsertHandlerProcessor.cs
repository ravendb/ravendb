using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.BulkInsert;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Documents.Sharding.Handlers.BulkInsert;
using Raven.Server.Documents.Sharding.Operations.BulkInsert;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.BulkInsert;

internal class ShardedBulkInsertHandlerProcessor : AbstractBulkInsertHandlerProcessor<ShardedBatchCommandData, ShardedBulkInsertHandler, TransactionOperationContext>, IAsyncDisposable
{
    private const string SampleChangeVector = "A:2568-F9I6Egqwm0Kz+K0oFVIR9Q";

    private readonly IDisposable _returnContext;
    private readonly ShardedBulkInsertOperation _operation;
    private readonly CancellationTokenSource _cts;

    public ShardedBulkInsertHandlerProcessor([NotNull] ShardedBulkInsertHandler requestHandler, [NotNull] JsonContextPoolBase<TransactionOperationContext> contextPool, ShardedDatabaseContext databaseContext,
        long operationId, bool skipOverwriteIfUnchanged, CancellationToken token) :
        base(requestHandler, contextPool, null, skipOverwriteIfUnchanged, token)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(token, requestHandler.AbortRequestToken);
        _returnContext = contextPool.AllocateOperationContext(out TransactionOperationContext context);
        _operation = new ShardedBulkInsertOperation(operationId, skipOverwriteIfUnchanged, databaseContext, context, _cts.Token);
    }

    protected override AbstractBulkInsertBatchCommandsReader<ShardedBatchCommandData> GetCommandsReader(JsonOperationContext context, Stream requestBodyStream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
    {
        return new ShardedBulkInsertCommandsReader(context, requestBodyStream, buffer, token);
    }

    protected override async ValueTask ExecuteCommands(Task currentTask, int numberOfCommands, ShardedBatchCommandData[] array, long totalSize)
    {
        foreach (var readCommand in array)
        {
            if (readCommand is null)
                break;

            using (readCommand)
            {
                await _operation.StoreAsync(readCommand, readCommand.Data.Id);
            }
        }
    }

    protected override StreamsTempFile GetTempFile()
    {
        return RequestHandler.ServerStore.GetTempFile("attachment", "sharded");
    }

    protected override async ValueTask<string> CopyAttachmentStream(Stream stream, Stream attachmentStream)
    {
        await stream.CopyToAsync(attachmentStream, _cts.Token);
        return null;
    }

    protected override (long, int) GetSizeAndOperationsCount(ShardedBatchCommandData commandData)
    {
        return GetSizeAndOperationsCount(commandData.Data, estimatedChangeVectorSize: SampleChangeVector.Length);
    }

    public async ValueTask DisposeAsync()
    {
        base.Dispose();

        await using (_operation)
        using (_returnContext)
        {

        }

        _cts.Dispose();
    }
}
