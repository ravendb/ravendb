using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
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
