using System;
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
        BatchRequestParser.CommandParsingObserver = _commandStreamCopier = new BatchCommandStreamCopier();
    }

    public override async Task<ShardedBatchCommandData> GetCommandAsync(JsonOperationContext ctx, BlittableMetadataModifier modifier)
    {
        var command = new ShardedBatchCommandData(ctx);

        using (_commandStreamCopier.UseStream(command.Stream))
        {
            var moveNext = MoveNext(ctx, modifier);

            if (moveNext == null)
                return null;

            command.Data = await moveNext;

            return command;
        }
    }
}
