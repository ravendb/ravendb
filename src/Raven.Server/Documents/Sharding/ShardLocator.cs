using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static DocumentIdsByShardIndex GroupIdsByShardIndex(IEnumerable<Slice> ids, ShardedDatabaseContext databaseContext)
        {
            var result = new DocumentIdsByShardIndex();

            foreach (var id in ids)
            {
                var shardIndex = databaseContext.GetShardIndex(id);
                result.Add(shardIndex, id);
            }

            return result;
        }

        public static Dictionary<int, List<int>> GetDocumentIdsShards(IList<string> ids,
            ShardedDatabaseContext databaseContext, TransactionOperationContext context)
        {
            var result  = new Dictionary<int, List<int>>();

            for (var i = 0; i < ids.Count; i++)
            {
                var id = ids[i];
                var shardId = ShardedDatabaseContext.GetShardId(context, id);
                var index = databaseContext.GetShardIndex(shardId);

                if (result.TryGetValue(index, out var shardIds) == false)
                {
                    shardIds = new List<int>();
                    result.Add(index, shardIds);
                }

                shardIds.Add(i);
            }

            return result;
        }

        public class DocumentIdsByShardIndex : IEnumerable<(int ShardId, List<Slice> DocumentIds)>
        {
            private readonly Dictionary<int, List<Slice>> _dictionary;

            public DocumentIdsByShardIndex()
            {
                _dictionary = new Dictionary<int, List<Slice>>();
            }

            public void Add(int shardIndex, Slice documentId)
            {
                if (_dictionary.TryGetValue(shardIndex, out var idsInShard) == false)
                {
                    _dictionary[shardIndex] = idsInShard = new List<Slice>();
                }

                idsInShard.Add(documentId);
            }

            public IEnumerator<(int ShardId, List<Slice> DocumentIds)> GetEnumerator()
            {
                foreach ((int shardId, List<Slice> list) in _dictionary)
                {
                    yield return (shardId, list);
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
