using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;


namespace Raven.Server.Utils
{
    public static class ShardHelper
    {
        public static int TryGetShardIndexAndDatabaseName(ref string name)
        {
            int shardIndex = name.IndexOf('$');
            if (shardIndex != -1)
            {
                var slice = name.AsSpan().Slice(shardIndex + 1);
                name = name.Substring(0, shardIndex);
                if (int.TryParse(slice, out shardIndex) == false)
                    throw new ArgumentNullException(nameof(name), "Unable to parse sharded database name: " + name);
            }

            return shardIndex;
        }

        public static int TryGetShardIndex(string name)
        {
            int shardIndex = name.IndexOf('$');
            if (shardIndex != -1)
            {
                var slice = name.AsSpan().Slice(shardIndex + 1);
                if (int.TryParse(slice, out shardIndex) == false)
                    throw new ArgumentNullException(nameof(name), "Unable to parse sharded database name: " + name);
            }

            return shardIndex;
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardIndex = shardName.IndexOf('$');
            if (shardIndex == -1)
                return shardName;

            return shardName.Substring(0, shardIndex);
        }

        public static bool IsShardedName(string name)
        {
            return name.IndexOf('$') != -1;
        }

        public static IEnumerable<string> GetShardNames(DatabaseRecord record)
        {
            for (int i = 0; i < record.Shards.Length; i++)
            {
                yield return $"{record.DatabaseName}${i}";
            }
        }

        public const int NumberOfShards = 1024 * 1024;

        /// <summary>
        /// The shard id is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public static int GetShardId(TransactionOperationContext context, string key)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, key, out var lowerId, out _))
            {
                unsafe
                {
                    byte* buffer = lowerId.Content.Ptr;
                    int size = lowerId.Size;

                    AdjustAfterSeparator((byte)'$', ref buffer, ref size);

                    if (size == 0)
                        throw new ArgumentException("Key '" + key + "', has a shard id length of 0");

                    var hash = Hashing.XXHash64.Calculate(buffer, (ulong)size);
                    return (int)(hash % NumberOfShards);
                }
            }
        }

        private static unsafe void AdjustAfterSeparator(byte expected, ref byte* ptr, ref int len)
        {
            for (int i = len - 1; i > 0; i--)
            {
                if (ptr[i] != expected)
                    continue;
                ptr += i + 1;
                len -= i - 1;
                break;
            }
        }

        public static int GetShardIndex(List<DatabaseRecord.ShardRangeAssignment> shardAllocation, int id)
        {
            for (int i = 0; i < shardAllocation.Count - 1; i++)
            {
                if (id < shardAllocation[i + 1].RangeStart)
                {
                    return i ;
                }
            }
            return shardAllocation.Count - 1 ;
        }

        public static int GetShardIndexforDocument(TransactionOperationContext context, List<DatabaseRecord.ShardRangeAssignment> shardAllocation, string docId)
        {
            return GetShardIndex(shardAllocation, GetShardId(context, docId));
        }
    }
}
