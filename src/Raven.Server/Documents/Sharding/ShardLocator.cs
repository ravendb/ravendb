using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Primitives;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static Dictionary<int, List<string>> GroupIdsByShardIndex(List<string> ids,
            ShardedContext shardedContext, TransactionOperationContext context)
        {
            var result  = new Dictionary<int, List<string>>();

            foreach (var id in ids)
            {
                var shardIndex = shardedContext.GetShardIndex(context, id);

                if (result.TryGetValue(shardIndex, out var idsInShard) == false)
                {
                    result[shardIndex] = idsInShard = new List<string>();
                }

                idsInShard.Add(id);
            }

            return result;
        }
        public static Dictionary<int, List<int>> GetDocumentIdsShards(IList<string> ids,
            ShardedContext shardedContext, TransactionOperationContext context)
        {
            var result  = new Dictionary<int, List<int>>();

            for (var i = 0; i < ids.Count; i++)
            {
                var id = ids[i];
                var shardId = shardedContext.GetShardId(context, id);
                var index = shardedContext.GetShardIndex(shardId);

                if (result.TryGetValue(index, out var shardIds) == false)
                {
                    shardIds = new List<int>();
                    result.Add(index, shardIds);
                }

                shardIds.Add(i);
            }

            return result;
        }

    }
}
