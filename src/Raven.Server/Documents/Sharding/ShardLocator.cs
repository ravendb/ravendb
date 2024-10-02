using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static Dictionary<int, IdsByShard<string>> GetDocumentIdsByShards(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<ReadOnlyMemory<char>> ids) =>
            GetDocumentIdsByShardsGeneric(context, databaseContext, ConvertToStringEnumerable(ids));

        public static Dictionary<int, IdsByShard<string>> GetDocumentIdsByShards(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<string> ids) =>
            GetDocumentIdsByShardsGeneric(context, databaseContext, ids);

        public static Dictionary<int, IdsByShard<Slice>> GetDocumentIdsByShards(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<Slice> ids) =>
            GetDocumentIdsByShardsGeneric(context, databaseContext, ids);
      
        public static Dictionary<int, IdsByShard<string>> GetDocumentIdsByShards(ShardedDatabaseContext databaseContext, IEnumerable<string> ids)
        {
            using (databaseContext.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                return GetDocumentIdsByShardsGeneric(context, databaseContext, ids);
            }
        }

        private static Dictionary<int, IdsByShard<T>> GetDocumentIdsByShardsGeneric<T>(TransactionOperationContext context, ShardedDatabaseContext databaseContext, IEnumerable<T> ids)
        {
            var result = new Dictionary<int, IdsByShard<T>>();
            var i = 0;
            foreach (var id in ids)
            {
                int shardNumber;
                if (typeof(T) == typeof(Slice))
                {
                    shardNumber = databaseContext.GetShardNumberFor((Slice)(object)id);
                }
                else if (typeof(T) == typeof(string))
                {
                    shardNumber = databaseContext.GetShardNumberFor(context, (string)(object)id);
                }
                else
                {
                    throw new ArgumentException($"the type {typeof(T).FullName} not supported for bucket calculation");
                }


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

        public sealed class IdsByShard<T>
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

        private static IEnumerable<string> ConvertToStringEnumerable(IEnumerable<ReadOnlyMemory<char>> items)
        {
            foreach (var item in items)
                yield return item.ToString();
        }
    }
}
