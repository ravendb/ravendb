using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static DocumentIdsByShardIndex GroupIdsByShardIndex(IEnumerable<string> ids,
            ShardedContext shardedContext, TransactionOperationContext context)
        {
            var result = new DocumentIdsByShardIndex();

            foreach (var id in ids)
            {
                var shardIndex = shardedContext.GetShardIndex(context, id);
                result.Add(shardIndex, id);
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

        public class DocumentIdsByShardIndex : IEnumerable<(int ShardId, List<string> DocumentIds)>
        {
            private readonly Dictionary<int, List<string>> _dictionary;

            public DocumentIdsByShardIndex()
            {
                _dictionary = new Dictionary<int, List<string>>();
            }

            public void Add(int shardIndex, string documentId)
            {
                if (_dictionary.TryGetValue(shardIndex, out var idsInShard) == false)
                {
                    _dictionary[shardIndex] = idsInShard = new List<string>();
                }

                idsInShard.Add(documentId);
            }

            public IEnumerator<(int ShardId, List<string> DocumentIds)> GetEnumerator()
            {
                foreach ((int shardId, List<string> list) in _dictionary)
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
