using System.Collections;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardLocator
    {
        public static DocumentIdsByShardNumber GroupIdsByShardNumber(IEnumerable<Slice> ids, ShardedDatabaseContext shardedDatabaseContext, TransactionOperationContext context)
        {
            var result = new DocumentIdsByShardNumber();

            foreach (var id in ids)
            {
                var bucket = ShardHelper.GetBucket(context, id);

                var shardNumber = shardedDatabaseContext.GetShardNumber(bucket);
                result.Add(shardNumber, id);
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

                var shardNumber = databaseContext.GetShardNumber(context, id);

                if (result.TryGetValue(shardNumber, out var shardNumbers) == false)
                {
                    shardNumbers = new List<int>();
                    result.Add(shardNumber, shardNumbers);
                }

                shardNumbers.Add(i);
            }

            return result;
        }

        public class DocumentIdsByShardNumber : IEnumerable<(int ShardId, List<Slice> DocumentIds)>
        {
            private readonly Dictionary<int, List<Slice>> _dictionary;

            public DocumentIdsByShardNumber()
            {
                _dictionary = new Dictionary<int, List<Slice>>();
            }

            public void Add(int shardNumber, Slice documentId)
            {
                if (_dictionary.TryGetValue(shardNumber, out var idsInShard) == false)
                {
                    _dictionary[shardNumber] = idsInShard = new List<Slice>();
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
