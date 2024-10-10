using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Sharding;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Utils
{
    public static class ShardHelper
    {
        public const int NumberOfBuckets = ClientShardHelper.NumberOfBuckets;

        /// <summary>
        /// The bucket is a hash of the document id, lower case, reduced to
        /// 20 bits. This gives us 0 .. 1M range of shard ids and means that assuming
        /// perfect distribution of data, each shard is going to have about 1MB of data
        /// per TB of overall db size. That means that even for *very* large databases, the
        /// size of the shard is still going to be manageable.
        /// </summary>
        public static int GetBucketFor(ShardingConfiguration configuration, ByteStringContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
            {
                return GetBucketFor(configuration, lowerId);
            }
        }

        public static int GetBucketFor(ShardingConfiguration configuration, Slice lowerId) => GetBucketFor(configuration, lowerId.AsReadOnlySpan());

        public static int GetBucketFor(ShardingConfiguration configuration, ReadOnlySpan<byte> lowerId) => ClientShardHelper.GetBucketFor(configuration, lowerId);
        private static unsafe void AdjustAfterSeparator(char expected, ref char* ptr, ref int len)
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

        public static bool TryGetShardNumberAndDatabaseName(string databaseName, out string shardedDatabaseName, out int shardNumber) =>
            ClientShardHelper.TryGetShardNumberAndDatabaseName(databaseName, out shardedDatabaseName, out shardNumber);

        public static bool TryGetShardNumberFromDatabaseName(string shardedDatabaseName, out int shardNumber)
        {
            shardNumber = GetShardNumberFromDatabaseName(shardedDatabaseName, throwIfShardNumberNotFound: false);

            if (shardNumber != -1)
                return true;

            return false;
        }

        public static int GetShardNumberFromDatabaseName(string shardedDatabaseName, bool throwIfShardNumberNotFound = true)
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

        public static bool IsShardName(string name) => ClientShardHelper.IsShardName(name);

        public static IEnumerable<string> GetShardNames(DatabaseRecord record)
        {
            var recordDatabaseName = record.DatabaseName;
            var shards = record.Sharding.Shards.Keys.AsEnumerable();

            return GetShardNames(recordDatabaseName, shards);
        }

        public static IEnumerable<string> GetShardNames(string databaseName, IEnumerable<int> shards)
        {
            foreach (var shardNumber in shards)
            {
                yield return $"{databaseName}${shardNumber}";
            }
        }

        public static int GetShardNumberFor(ShardingConfiguration configuration, int bucket) => FindBucketShard(configuration.BucketRanges, bucket);

        public static int GetShardNumberFor(ShardingConfiguration configuration, ByteStringContext allocator, string id) => GetShardNumberAndBucketFor(configuration, allocator, id).ShardNumber;

        public static int GetShardNumberFor(ShardingConfiguration configuration, ByteStringContext allocator, LazyStringValue id) => GetShardNumberAndBucketFor(configuration, allocator, id).ShardNumber;

        public static int GetShardNumberFor<TTransaction>(ShardingConfiguration configuration, TransactionOperationContext<TTransaction> context, string id)
            where TTransaction : RavenTransaction => GetShardNumberFor(configuration, context.Allocator, id);

        public static int GetShardNumberFor<TTransaction>(RawShardingConfiguration configuration, TransactionOperationContext<TTransaction> context, string id)
            where TTransaction : RavenTransaction => GetShardNumberAndBucketFor(configuration, context, id).ShardNumber;

        public static int GetShardNumberFor(ShardingConfiguration configuration, Slice id) => GetShardNumberAndBucketFor(configuration, id).ShardNumber;

        private static (int ShardNumber, int Bucket) GetShardNumberAndBucketFor(ShardingConfiguration configuration, Slice id)
        {
            int bucket = GetBucketFor(configuration, id);
            return (FindBucketShard(configuration.BucketRanges, bucket), bucket);
        }

        public static (int ShardNumber, int Bucket) GetShardNumberAndBucketFor(ShardingConfiguration configuration, ByteStringContext allocator, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(allocator, id, out var lowerId))
            {
                return GetShardNumberAndBucketFor(configuration, lowerId);
            }
        }

        public static (int ShardNumber, int Bucket) GetShardNumberAndBucketFor(ShardingConfiguration configuration, ByteStringContext allocator, LazyStringValue id)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Avoid the allocation of the LazyStringValue below");
            using (DocumentIdWorker.GetSliceFromId(allocator, id, out var lowerId))
            {
                return GetShardNumberAndBucketFor(configuration, lowerId);
            }
        }

        public static (int ShardNumber, int Bucket) GetShardNumberAndBucketFor<TTransaction>(ShardingConfiguration configuration, TransactionOperationContext<TTransaction> context, string id)
            where TTransaction : RavenTransaction
        {
            return GetShardNumberAndBucketFor(configuration, context.Allocator, id);
        }

        public static (int ShardNumber, int Bucket) GetShardNumberAndBucketFor<TTransaction>(RawShardingConfiguration configuration, TransactionOperationContext<TTransaction> context, string id)
            where TTransaction : RavenTransaction
        {
            int bucket = GetBucketFor(configuration.MaterializedConfiguration, context.Allocator, id);
            return (FindBucketShard(configuration.BucketRanges, bucket), bucket);
        }

        private static int FindBucketShard(List<ShardBucketRange> ranges, int bucket) => ClientShardHelper.FindBucketShard(ranges, bucket);

        public static void MoveBucket(this DatabaseRecord record, int bucket, int toShard)
        {
            try
            {
                if (bucket >= NumberOfBuckets)
                {
                    if (record.Sharding.Prefixed is not { Count: > 0 })
                        throw new InvalidOperationException($"For database '{record.DatabaseName}' total number of buckets is {NumberOfBuckets}, requested: {bucket}");

                    // prefixed range
                }

                if (bucket == 0)
                {
                    if (record.Sharding.BucketRanges[0].ShardNumber == toShard)
                        return; // same shard

                    record.Sharding.BucketRanges[0].BucketRangeStart++;
                    record.Sharding.BucketRanges.Insert(0, new ShardBucketRange { BucketRangeStart = 0, ShardNumber = toShard });
                    return;
                }

                if (bucket == NumberOfBuckets - 1)
                {
                    if (record.Sharding.BucketRanges[^1].ShardNumber == toShard)
                        return; // same shard

                    record.Sharding.BucketRanges.Add(new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                    return;
                }

                for (int i = 0; i < record.Sharding.BucketRanges.Count - 1; i++)
                {
                    var start = record.Sharding.BucketRanges[i].BucketRangeStart;
                    var end = record.Sharding.BucketRanges[i + 1].BucketRangeStart - 1;
                    var size = end - start + 1;

                    if (bucket <= end)
                    {
                        var currentShard = record.Sharding.BucketRanges[i].ShardNumber;
                        if (currentShard == toShard)
                            return; // same shard

                        if (size == 1)
                        {
                            var next = record.Sharding.BucketRanges[i + 1].ShardNumber;
                            var prev = record.Sharding.BucketRanges[i - 1].ShardNumber;

                            if (next == toShard)
                            {
                                record.Sharding.BucketRanges[i + 1].BucketRangeStart--;
                                record.Sharding.BucketRanges.RemoveAt(i);
                            }

                            if (prev == toShard)
                            {
                                record.Sharding.BucketRanges.RemoveAt(i);
                            }

                            if (next != toShard && prev != toShard)
                                record.Sharding.BucketRanges[i].ShardNumber = toShard;

                            return;
                        }

                        if (bucket == start)
                        {
                            record.Sharding.BucketRanges[i].BucketRangeStart++;

                            if (record.Sharding.BucketRanges[i - 1].ShardNumber == toShard)
                                return;

                            record.Sharding.BucketRanges.Insert(i, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                            return;
                        }

                        if (bucket == end)
                        {
                            if (record.Sharding.BucketRanges[i + 1].ShardNumber == toShard)
                            {
                                record.Sharding.BucketRanges[i + 1].BucketRangeStart--;
                                return;
                            }

                            record.Sharding.BucketRanges.Insert(i + 1, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                            return;
                        }

                        // split
                        record.Sharding.BucketRanges.Insert(i + 1, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });

                        record.Sharding.BucketRanges.Insert(i + 2, new ShardBucketRange { BucketRangeStart = bucket + 1, ShardNumber = currentShard });
                        return;
                    }
                }

                var lastRange = record.Sharding.BucketRanges[^1];
                if (bucket == lastRange.BucketRangeStart)
                {
                    if (toShard == record.Sharding.BucketRanges[^2].ShardNumber)
                    {
                        record.Sharding.BucketRanges[^1].BucketRangeStart++;
                        return;
                    }
                }

                if (lastRange.ShardNumber != toShard)
                {
                    // split last
                    record.Sharding.BucketRanges.Insert(record.Sharding.BucketRanges.Count, new ShardBucketRange { BucketRangeStart = bucket, ShardNumber = toShard });
                    record.Sharding.BucketRanges.Insert(record.Sharding.BucketRanges.Count,
                        new ShardBucketRange { BucketRangeStart = bucket + 1, ShardNumber = lastRange.ShardNumber });
                }
            }
            finally
            {
                ValidateBucketsMapping(record);
            }
        }

        private static void ValidateBucketsMapping(DatabaseRecord record)
        {
            if (record.Sharding.BucketRanges[0].BucketRangeStart != 0)
                throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                    $"First mapping must start with zero, actual: {record.Sharding.BucketRanges[0].BucketRangeStart}");

            var lastShard = record.Sharding.BucketRanges[0].ShardNumber;
            var lastStart = 0;

            for (int i = 1; i < record.Sharding.BucketRanges.Count - 1; i++)
            {
                var current = record.Sharding.BucketRanges[i];
                if (current.BucketRangeStart <= lastStart)
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Overlap detected between mapping '{i}' and '{i - 1}' start: {current.BucketRangeStart}, previous end: {lastStart}");
                if (current.ShardNumber == lastShard)
                    throw new InvalidOperationException($"At database '{record.DatabaseName}', " +
                                                        $"Not merged shard continuous range detected between mapping '{i}' and '{i - 1}' at shard: {current.ShardNumber}");

                lastStart = current.BucketRangeStart;
                lastShard = current.ShardNumber;
            }
        }

        public static string GenerateStickyId(string id, char identityPartsSeparator)
        {
            Debug.Assert(id[^1] == identityPartsSeparator, "id[^1] == identityPartsSeparator");

            var builder = new StringBuilder(id);

            builder
                .Append('$');

            ChangeVectorExtensions.ToBase26(builder, Random.Shared.Next());

            builder
                .Append('$')
                .Append(identityPartsSeparator);

            return builder.ToString();
        }

        public static string GenerateStickyId(string id, string originalId, char identityPartsSeparator)
        {
            Debug.Assert(id[^1] == identityPartsSeparator, "id[^1] == identityPartsSeparator");

            var builder = new StringBuilder(id);
            var index = originalId.LastIndexOf('$');
            if (index != -1)
                originalId = originalId[index..originalId.Length];
            else
                builder.Append('$');

            builder.Append(originalId)
                .Append('$')
                .Append(identityPartsSeparator);

            return builder.ToString();
        }

        public static (int ShardNumber, int Bucket) GetShardNumberAndBucketForIdentity(ShardingConfiguration configuration, TransactionOperationContext context, string id, char identityPartsSeparator)
        {
            // the expected id format here is users/$BASE26$/
            // so we cut the '$/' from the end to detect shard number based on BASE26 part
            Debug.Assert(id[^1] == identityPartsSeparator, $"id[^1] != {identityPartsSeparator} for {id}");
            Debug.Assert(id[^2] == '$', $"id[^2] != $ for {id}");
            var actualId = id.AsSpan(0, id.Length - 2);
            var actualIdAsString = actualId.ToString();
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 Optimize this");
            return GetShardNumberAndBucketFor(configuration, context.Allocator, actualIdAsString);
        }

        public static int GetShardNumberFor(ShardingConfiguration configuration, TransactionOperationContext context, string id, char identityPartsSeparator)
        {
            // the expected id format here is users/$BASE26$/
            // so we cut the '$/' from the end to detect shard number based on BASE26 part
            if (id[^1] == identityPartsSeparator && id[^2] == '$')
            {
                var actualId = id.AsSpan(0, id.Length - 2);
                id = actualId.ToString();
            }
            
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Normal, "RavenDB-19086 Optimize this");
            return GetShardNumberAndBucketFor(configuration, context.Allocator, id).ShardNumber;
        }

        public static unsafe void ExtractStickyId(ref char* buffer, ref int size)
        {
            AdjustAfterSeparator('$', ref buffer, ref size);
        }
    }
}
