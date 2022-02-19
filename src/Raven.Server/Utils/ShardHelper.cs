using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Utils
{
    public static class ShardHelper
    {
        public const int NumberOfBuckets = 1024 * 1024;

        /// <summary>
        /// The bucket is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public static int GetBucket(string key)
        {
            using (var context = new ByteStringContext(SharedMultipleUseFlag.None))
            using (DocumentIdWorker.GetLower(context, key, out var lowerId))
            {
                return GetBucketFromSlice(lowerId);
            }
        }
        
        public static int GetBucket(TransactionOperationContext context, string key)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, key, out var lowerId, out _))
            {
                return GetBucketFromSlice(lowerId);
            }
        }

        public static int GetBucket(ByteStringContext context, LazyStringValue key)
        {
            using (DocumentIdWorker.GetLower(context, key, out var lowerId))
            {
                return GetBucketFromSlice(lowerId);
            }
        }

        //do not use this directly, we might mutate the slice buffer here.
        private static unsafe int GetBucketFromSlice(Slice lowerId)
        {
            byte* buffer = lowerId.Content.Ptr;
            int size = lowerId.Size;

            AdjustAfterSeparator((byte)'$', ref buffer, ref size);

            if (size == 0)
                throw new ArgumentException("Key '" + lowerId + "', has a shard id length of 0");

            var hash = Hashing.XXHash64.Calculate(buffer, (ulong)size);
            return (int)(hash % NumberOfBuckets);
        }

        private static unsafe void AdjustAfterSeparator(byte expected, ref byte* ptr, ref int len)
        {
            for (int i = len - 1; i > 0; i--)
            {
                if (ptr[i] != expected)
                    continue;
                ptr += i + 1;
                len -= i + 1;
                break;
            }
        }

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

        public static string ToDatabaseName(string shardName) => ClientShardHelper.ToDatabaseName(shardName);

        public static string ToShardName(string database, int shard) => ClientShardHelper.ToShardName(database, shard);

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

        public static int GetShardForId(TransactionOperationContext context, List<DatabaseRecord.ShardRangeAssignment> shardAllocation, string docId)
        {
            var bucket = GetBucket(context, docId);
            return GetShardIndex(shardAllocation, bucket);
        }

        public static int GetShardForId(TransactionOperationContext context, List<DatabaseRecord.ShardRangeAssignment> shardAllocation, LazyStringValue docId)
        {
            var bucket = GetBucket(context.Allocator, docId);
            return GetShardIndex(shardAllocation, bucket);
        }

        public static int GetShardIndex(List<DatabaseRecord.ShardRangeAssignment> ranges, int bucket)
        {
            for (int i = 0; i < ranges.Count - 1; i++)
            {
                if (bucket < ranges[i + 1].RangeStart)
                    return ranges[i].Shard;
            }

            return ranges[^1].Shard;
        }

        public static void MoveBucket(this DatabaseRecord record, int bucket, int toShard)
        {
            try
            {
                if (bucket >= NumberOfBuckets)
                    throw new InvalidOperationException($"For database '{record.DatabaseName}' total number of buckets is {NumberOfBuckets}, requested: {bucket}");

                if (bucket == 0)
                {
                    if (record.ShardAllocations[0].Shard == toShard)
                        return; // same shard

                    record.ShardAllocations[0].RangeStart++;
                    record.ShardAllocations.Insert(0, new DatabaseRecord.ShardRangeAssignment { RangeStart = 0, Shard = toShard });
                    return;
                }

                if (bucket == NumberOfBuckets - 1)
                {
                    if (record.ShardAllocations[^1].Shard == toShard)
                        return; // same shard

                    record.ShardAllocations.Add(new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket, Shard = toShard });
                    return;
                }

                for (int i = 0; i < record.ShardAllocations.Count - 1; i++)
                {
                    var start = record.ShardAllocations[i].RangeStart;
                    var end = record.ShardAllocations[i + 1].RangeStart - 1;
                    var size = end - start + 1;

                    if (bucket <= end)
                    {
                        var currentShard = record.ShardAllocations[i].Shard;
                        if (currentShard == toShard)
                            return; // same shard

                        if (size == 1)
                        {
                            var next = record.ShardAllocations[i + 1].Shard;
                            var prev = record.ShardAllocations[i - 1].Shard;

                            if (next == toShard)
                            {
                                record.ShardAllocations[i + 1].RangeStart--;
                                record.ShardAllocations.RemoveAt(i);
                            }

                            if (prev == toShard)
                            {
                                record.ShardAllocations.RemoveAt(i);
                            }

                            if (next != toShard && prev != toShard)
                                record.ShardAllocations[i].Shard = toShard;

                            return;
                        }

                        if (bucket == start)
                        {
                            record.ShardAllocations[i].RangeStart++;

                            if (record.ShardAllocations[i - 1].Shard == toShard)
                                return;

                            record.ShardAllocations.Insert(i + 1, new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket, Shard = toShard });
                            return;
                        }

                        if (bucket == end)
                        {
                            if (record.ShardAllocations[i + 1].Shard == toShard)
                            {
                                record.ShardAllocations[i + 1].RangeStart--;
                                return;
                            }

                            record.ShardAllocations.Insert(i + 1, new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket, Shard = toShard });
                            return;
                        }

                        // split
                        record.ShardAllocations.Insert(i + 1, new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket, Shard = toShard });

                        record.ShardAllocations.Insert(i + 2, new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket + 1, Shard = currentShard });
                        return;
                    }
                }

                var lastRange = record.ShardAllocations[^1];
                if (bucket == lastRange.RangeStart)
                {
                    if (toShard == record.ShardAllocations[^2].Shard)
                    {
                        record.ShardAllocations[^1].RangeStart++;
                        return;
                    }
                }

                // split last
                record.ShardAllocations.Insert(record.ShardAllocations.Count, new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket, Shard = toShard });
                record.ShardAllocations.Insert(record.ShardAllocations.Count,
                    new DatabaseRecord.ShardRangeAssignment { RangeStart = bucket + 1, Shard = lastRange.Shard });
            }
            finally
            {
                ValidateBucketsMapping(record);
            }
        }

        private static void ValidateBucketsMapping(DatabaseRecord record)
        {
            if (record.ShardAllocations[0].RangeStart != 0)
                throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                    $"First mapping must start with zero, actual: {record.ShardAllocations[0].RangeStart}");

            var lastShard = record.ShardAllocations[0].Shard;
            var lastStart = 0;

            for (int i = 1; i < record.ShardAllocations.Count - 1; i++)
            {
                var current = record.ShardAllocations[i];
                if (current.RangeStart <= lastStart)
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Overlap detected between mapping '{i}' and '{i-1}' start: {current.RangeStart}, previous end: {lastStart}");
                if (current.Shard == lastShard)                    
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Not merged shard continuous range detected between mapping '{i}' and '{i-1}' at shard: {current.Shard}");

                lastStart = current.RangeStart;
                lastShard = current.Shard;
            }
        }
    }
}
