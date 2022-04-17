using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
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
        public static int GetBucket(string id)
        {
            using (var context = new ByteStringContext(SharedMultipleUseFlag.None))
            using (DocumentIdWorker.GetLower(context, id, out var lowerId))
            {
                return GetBucketFromSlice(lowerId);
            }
        }
        
        public static int GetBucket(LazyStringValue id)
        {
            using (var context = new ByteStringContext(SharedMultipleUseFlag.None))
                return GetBucket(context, id);
        }

        private static int GetBucket<TTransaction>(TransactionOperationContext<TTransaction> context, string id) where TTransaction : RavenTransaction
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out var lowerId, out _))
            {
                return GetBucketFromSlice(lowerId);
            }
        }

        public static int GetBucket(ByteStringContext context, LazyStringValue id)
        {
            using (DocumentIdWorker.GetLower(context, id, out var lowerId))
            {
                return GetBucketFromSlice(lowerId);
            }
        }

        public static unsafe int GetBucket(TransactionOperationContext context, Slice id)
        {
            using (DocumentIdWorker.GetLower(context.Allocator, id.Content.Ptr, id.Size, out var loweredId))
            {
                return GetBucketFromSlice(loweredId);
            }
        }

        public static unsafe int GetBucket(byte* buffer, int size)
        {
            AdjustAfterSeparator((byte)'$', ref buffer, ref size);

            var hash = Hashing.XXHash64.Calculate(buffer, (ulong)size);
            return (int)(hash % NumberOfBuckets);
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

        public static bool TryGetShardNumberAndDatabaseName(ref string shardedDatabaseName, out int shardNumber)
        {
            shardNumber = shardedDatabaseName.IndexOf('$');

            if (shardNumber != -1)
            {
                var slice = shardedDatabaseName.AsSpan().Slice(shardNumber + 1);
                shardedDatabaseName = shardedDatabaseName.Substring(0, shardNumber);
                if (int.TryParse(slice, out shardNumber) == false)
                    throw new ArgumentException(nameof(shardedDatabaseName), "Unable to parse sharded database name: " + shardedDatabaseName);

                return true;
            }

            return false;
        }

        public static bool TryGetShardNumber(string shardedDatabaseName, out int shardNumber)
        {
            shardNumber = GetShardNumber(shardedDatabaseName, throwIfShardNumberNotFound: false);
            
            if (shardNumber != -1)
                return true;

            return false;
        }

        public static int GetShardNumber(string shardedDatabaseName, bool throwIfShardNumberNotFound = true)
        {
            var shardNumber = shardedDatabaseName.IndexOf('$');
            if (shardNumber != -1)
            {
                var slice = shardedDatabaseName.AsSpan().Slice(shardNumber + 1);
                if (int.TryParse(slice, out shardNumber) == false)
                    throw new ArgumentException(nameof(shardedDatabaseName), "Unable to parse sharded database name: " + shardedDatabaseName);
            }
            else
            {
                if (throwIfShardNumberNotFound)
                    throw new ArgumentException(nameof(shardedDatabaseName), "It is not sharded database name: " + shardedDatabaseName);
            }

            return shardNumber;
        }

        public static string ToDatabaseName(string shardName) => ClientShardHelper.ToDatabaseName(shardName);

        public static string ToShardName(string database, int shard) => ClientShardHelper.ToShardName(database, shard);

        public static bool IsShardedName(string name)
        {
            return name.IndexOf('$') != -1;
        }

        public static IEnumerable<string> GetShardNames(DatabaseRecord record)
        {
            var recordDatabaseName = record.DatabaseName;
            var shardsLength = record.Sharding.NumberOfShards;

            return GetShardNames(recordDatabaseName, shardsLength);
        }

        public static IEnumerable<string> GetShardNames(string databaseName, int shardsCount)
        {
            for (int i = 0; i < shardsCount; i++)
            {
                yield return $"{databaseName}${i}";
            }
        }

        public static int FindBucketShard(List<ShardBucketRange> ranges, int bucket)
        {
            for (int i = 0; i < ranges.Count - 1; i++)
            {
                if (bucket < ranges[i + 1].BucketRangeStart)
                    return ranges[i].ShardNumber;
            }

            return ranges[^1].ShardNumber;
        }

        public static void MoveBucket(this DatabaseRecord record, int bucket, int toShard)
        {
            try
            {
                if (bucket >= NumberOfBuckets)
                    throw new InvalidOperationException($"For database '{record.DatabaseName}' total number of buckets is {NumberOfBuckets}, requested: {bucket}");

                if (bucket == 0)
                {
                    if (record.Sharding.ShardBucketRanges[0].ShardNumber == toShard)
                        return; // same shard

                    record.Sharding.ShardBucketRanges[0].BucketRangeStart++;
                    record.Sharding.ShardBucketRanges.Insert(0, new ShardBucketRange { BucketRangeStart = 0, ShardNumber = toShard });
                    return;
                }

                if (bucket == NumberOfBuckets - 1)
                {
                    if (record.Sharding.ShardBucketRanges[^1].ShardNumber == toShard)
                        return; // same shard

                    record.Sharding.ShardBucketRanges.Add(new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                    return;
                }

                for (int i = 0; i < record.Sharding.ShardBucketRanges.Count - 1; i++)
                {
                    var start = record.Sharding.ShardBucketRanges[i].BucketRangeStart;
                    var end = record.Sharding.ShardBucketRanges[i + 1].BucketRangeStart - 1;
                    var size = end - start + 1;

                    if (bucket <= end)
                    {
                        var currentShard = record.Sharding.ShardBucketRanges[i].ShardNumber;
                        if (currentShard == toShard)
                            return; // same shard

                        if (size == 1)
                        {
                            var next = record.Sharding.ShardBucketRanges[i + 1].ShardNumber;
                            var prev = record.Sharding.ShardBucketRanges[i - 1].ShardNumber;

                            if (next == toShard)
                            {
                                record.Sharding.ShardBucketRanges[i + 1].BucketRangeStart--;
                                record.Sharding.ShardBucketRanges.RemoveAt(i);
                            }

                            if (prev == toShard)
                            {
                                record.Sharding.ShardBucketRanges.RemoveAt(i);
                            }

                            if (next != toShard && prev != toShard)
                                record.Sharding.ShardBucketRanges[i].ShardNumber = toShard;

                            return;
                        }

                        if (bucket == start)
                        {
                            record.Sharding.ShardBucketRanges[i].BucketRangeStart++;

                            if (record.Sharding.ShardBucketRanges[i - 1].ShardNumber == toShard)
                                return;

                            record.Sharding.ShardBucketRanges.Insert(i + 1, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                            return;
                        }

                        if (bucket == end)
                        {
                            if (record.Sharding.ShardBucketRanges[i + 1].ShardNumber == toShard)
                            {
                                record.Sharding.ShardBucketRanges[i + 1].BucketRangeStart--;
                                return;
                            }

                            record.Sharding.ShardBucketRanges.Insert(i + 1, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                            return;
                        }

                        // split
                        record.Sharding.ShardBucketRanges.Insert(i + 1, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });

                        record.Sharding.ShardBucketRanges.Insert(i + 2, new ShardBucketRange { BucketRangeStart = bucket + 1, ShardNumber = currentShard });
                        return;
                    }
                }

                var lastRange = record.Sharding.ShardBucketRanges[^1];
                if (bucket == lastRange.BucketRangeStart)
                {
                    if (toShard == record.Sharding.ShardBucketRanges[^2].ShardNumber)
                    {
                        record.Sharding.ShardBucketRanges[^1].BucketRangeStart++;
                        return;
                    }
                }

                // split last
                record.Sharding.ShardBucketRanges.Insert(record.Sharding.ShardBucketRanges.Count, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                record.Sharding.ShardBucketRanges.Insert(record.Sharding.ShardBucketRanges.Count,
                    new ShardBucketRange { BucketRangeStart = bucket + 1, ShardNumber = lastRange.ShardNumber });
            }
            finally
            {
                ValidateBucketsMapping(record);
            }
        }

        private static void ValidateBucketsMapping(DatabaseRecord record)
        {
            if (record.Sharding.ShardBucketRanges[0].BucketRangeStart != 0)
                throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                    $"First mapping must start with zero, actual: {record.Sharding.ShardBucketRanges[0].BucketRangeStart}");

            var lastShard = record.Sharding.ShardBucketRanges[0].ShardNumber;
            var lastStart = 0;

            for (int i = 1; i < record.Sharding.ShardBucketRanges.Count - 1; i++)
            {
                var current = record.Sharding.ShardBucketRanges[i];
                if (current.BucketRangeStart <= lastStart)
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Overlap detected between mapping '{i}' and '{i-1}' start: {current.BucketRangeStart}, previous end: {lastStart}");
                if (current.ShardNumber == lastShard)                    
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Not merged shard continuous range detected between mapping '{i}' and '{i-1}' at shard: {current.ShardNumber}");

                lastStart = current.BucketRangeStart;
                lastShard = current.ShardNumber;
            }
        }

        public static int GetShardNumberFor(ShardingRecord sharding, string id)
        {
            int bucket = GetBucket(id);
            return FindBucketShard(sharding.For(id), bucket);
        }
        
        public static int GetShardNumberFor(ShardingRecord sharding, Slice id)
        {
            int bucket = GetBucketFromSlice(id);
            return FindBucketShard(sharding.For(id), bucket);
        }

        public static int GetShardNumberFor(ShardingRecord sharding, ByteStringContext allocator, LazyStringValue id)
        {
            int bucket = GetBucket(allocator, id);
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Avoid the allocation of the LazyStringValue below");
            return FindBucketShard(sharding.For(id), bucket); // todo: we have silent allocation here that we need to prevent
        }

        
        public static int GetShardNumberFor<TTransaction>(ShardingRecord sharding, TransactionOperationContext<TTransaction> context, string id)
            where TTransaction : RavenTransaction
        {
            int bucket = GetBucket(context, id);
            return FindBucketShard(sharding.For(id), bucket);
        }
    }
}
