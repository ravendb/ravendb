using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.BulkInsert;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertCommandsReader : AbstractBulkInsertBatchCommandsReader<ShardedBatchCommandData>
{
    private readonly BatchCommandStreamCopier _commandStreamCopier;

    public ShardedBulkInsertCommandsReader(JsonOperationContext ctx, Stream stream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token) : base(ctx, stream, buffer, new BatchRequestParser(), token)
    {
        BatchRequestParser.CommandParsingObserver = _commandStreamCopier = new BatchCommandStreamCopier(ctx);
    }

    public override Task<ShardedBatchCommandData> GetCommandAsync(JsonOperationContext ctx, BlittableMetadataModifier modifier)
    {
        var moveNext = MoveNext(ctx, modifier);

        if (moveNext == null)
            return null; // TODO arek

        return CopyCommandStream(ctx, moveNext);
    }

    private async Task<ShardedBatchCommandData> CopyCommandStream(JsonOperationContext ctx, Task<BatchRequestParser.CommandData> task)
    {
        var command = await task;

        var shardedCommandData = new ShardedBatchCommandData(command, ctx);

        await _commandStreamCopier.CopyToAsync(shardedCommandData.Stream);

        return shardedCommandData;
    }

    public override void Dispose()
    {
        base.Dispose();

        _commandStreamCopier.Dispose();
    }
}
