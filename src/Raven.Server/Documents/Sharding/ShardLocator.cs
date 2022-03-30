using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static Dictionary<int, IdsByShard<string>> GetDocumentIdsByShards(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<string> ids) =>
            GetDocumentIdsByShardsGeneric(context, databaseContext, ids);

        public static Dictionary<int, IdsByShard<Slice>> GetDocumentIdsByShards(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<Slice> ids) =>
            GetDocumentIdsByShardsGeneric(context, databaseContext, ids);

        private static Dictionary<int, IdsByShard<T>> GetDocumentIdsByShardsGeneric<T>(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<T> ids)
        {
            var result = new Dictionary<int, IdsByShard<T>>();
            var i = 0;
            foreach (var id in ids)
            {
                int bucket;
                if (typeof(T) == typeof(Slice))
                {
                    bucket = ShardHelper.GetBucket(context, (Slice)(object)id);
                }
                else if (typeof(T) == typeof(string))
                {
                    bucket = ShardHelper.GetBucket(context, (string)(object)id);
                }
                else
                {
                    throw new ArgumentException($"the type {typeof(T).FullName} not supported for bucket calculation");
                }

                var shardNumber = databaseContext.GetShardNumber(bucket);

                if (result.TryGetValue(shardNumber, out var idsForShard) == false)
                {
                    idsForShard = new IdsByShard<T>();
                    result.Add(shardNumber, idsForShard);
                }

                idsForShard.Add(id, i);
                i++;
            }

            return result;
        }

        public class IdsByShard<T>
        {
            public List<T> Ids;
            public List<int> Positions; // positions in the final result

            public void Add(T id, int position)
            {
                Ids ??= new List<T>();
                Positions ??= new List<int>();

                Ids.Add(id);
                Positions.Add(position);
            }
        }
    }
}
