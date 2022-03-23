using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.BulkInsert;

public class BulkInsertBatchCommandsReader : AbstractBulkInsertBatchCommandsReader<BatchRequestParser.CommandData, JsonOperationContext>
{
    public BulkInsertBatchCommandsReader(JsonOperationContext ctx, Stream stream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
    : base(ctx, stream, buffer, BatchRequestParser.Instance, token)
    {
    }

    public override Task<BatchRequestParser.CommandData> GetCommandAsync(JsonOperationContext ctx, BlittableMetadataModifier modifier)
    {
        return MoveNext(ctx, modifier);
    }
}
