using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static ShardLocatorResults GetDocumentIdsShards(IEnumerable<string> ids,
            ShardedContext shardedContext, TransactionOperationContext context)
        {
            ShardLocatorResults result = new ShardLocatorResults
            {
                ShardsToIds = new Dictionary<int, List<string>>(),
                IdsToShardPosition = new Dictionary<string, (int ShardId, int Position)>()
            };

            foreach (var id in ids)
            {
                var shardId = shardedContext.GetShardId(context, id);
                var index = shardedContext.GetShardIndex(shardId);

                if (result.ShardsToIds.TryGetValue(index, out var shardIds) == false)
                {
                    shardIds = new List<string>();
                    result.ShardsToIds.Add(index, shardIds);
                }

                result.IdsToShardPosition.Add(id, (index, shardIds.Count));
                shardIds.Add(id);
            }

            return result;
        }

        public class ShardLocatorResults
        {
            public Dictionary<int, List<string>> ShardsToIds { get; set; }
            public Dictionary<string, (int ShardId, int Position)> IdsToShardPosition { get; set; }
        }
    }
}
