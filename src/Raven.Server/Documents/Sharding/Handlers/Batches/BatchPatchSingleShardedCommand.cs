using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public sealed class BatchPatchSingleShardedCommand : SingleShardedCommand
{
    public List<(string Id, string ChangeVector)> List;

    public BufferedCommand BufferedCommand;

    public override IEnumerable<BatchPatchSingleShardedCommand> Retry(ShardedDatabaseContext databaseContext, TransactionOperationContext context)
    {
        var idsByShard = new Dictionary<int, List<(string Id, string ChangeVector)>>();
        foreach (var item in List)
        {
            var result = databaseContext.GetShardNumberAndBucketFor(context, item.Id);
            if (idsByShard.TryGetValue(result.ShardNumber, out var list) == false)
                idsByShard[result.ShardNumber] = list = new List<(string Id, string ChangeVector)>();

            list.Add(item);
        }

        foreach (var kvp in idsByShard)
        {
            var list = kvp.Value;
            yield return new BatchPatchSingleShardedCommand
            {
                ShardNumber = kvp.Key,
                BufferedCommand = BufferedCommand,
                List = list,
                CommandStream = BufferedCommand.ModifyBatchPatchStream(list),
                PositionInResponse = PositionInResponse
            };
        }
    }
}
