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

internal sealed class ShardedBulkInsertHandlerProcessor : AbstractBulkInsertHandlerProcessor<ShardedBatchCommandData, ShardedBulkInsertHandler, TransactionOperationContext>, IAsyncDisposable
{
    private readonly ShardedDatabaseContext _databaseContext;
    private const string SampleChangeVector = "A:2568-F9I6Egqwm0Kz+K0oFVIR9Q";

    private readonly ShardedBulkInsertOperation _operation;

    public ShardedBulkInsertHandlerProcessor([NotNull] ShardedBulkInsertHandler requestHandler, ShardedDatabaseContext databaseContext,
        long operationId, bool skipOverwriteIfUnchanged, CancellationToken token) 
        : base(requestHandler, null, skipOverwriteIfUnchanged, token)
    {
        _databaseContext = databaseContext;
        _operation = new ShardedBulkInsertOperation(operationId, skipOverwriteIfUnchanged, requestHandler, databaseContext, requestHandler.ContextPool, CancellationToken);
    }

    protected override AbstractBulkInsertBatchCommandsReader<ShardedBatchCommandData> GetCommandsReader(JsonOperationContext context, Stream requestBodyStream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
    {
        return new ShardedBulkInsertCommandsReader(context, requestBodyStream, buffer, token);
    }

    protected override async ValueTask ExecuteCommands(Task currentTask, int numberOfCommands, ShardedBatchCommandData[] array, long totalSize)
    {
        for (int i = 0; i < numberOfCommands; i++)
        {
            var command = array[i];

            using (command)
            {
                await _operation.StoreAsync(command, command.Data.Id);
            }
        }
    }

    protected override StreamsTempFile GetTempFile()
    {
        return RequestHandler.ServerStore.GetTempFile("attachment", "sharded-bulk-insert", _databaseContext.Encrypted);
    }

    protected override async ValueTask<string> CopyAttachmentStreamAsync(Stream stream, Stream attachmentStream, CancellationToken token)
    {
        await stream.CopyToAsync(attachmentStream, token);
        return null;
    }

    protected override (long, int) GetSizeAndOperationsCount(ShardedBatchCommandData commandData)
    {
        return GetSizeAndOperationsCount(commandData.Data, estimatedChangeVectorSize: SampleChangeVector.Length);
    }

    protected override async Task OnErrorAsync(Exception exception)
    {
        await _operation.AbortAsync();
    }

    public async ValueTask DisposeAsync()
    {
        base.Dispose();

        await _operation.DisposeAsync();
    }
}
