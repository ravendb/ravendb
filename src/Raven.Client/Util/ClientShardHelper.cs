using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.ServerWide.Sharding;
using Sparrow;

namespace Raven.Client.Util
{
    internal static class ClientShardHelper
    {
        public const int NumberOfBuckets = 1024 * 1024;

        public static string ToShardName(string database, int shardNumber)
        {
            if (IsShardName(database))
            {
                Debug.Assert(false, $"Expected a non shard name but got {database}");
                throw new ArgumentException($"Expected a non shard name but got {database}");
            }

            ResourceNameValidator.AssertValidDatabaseName(database);
            if (shardNumber < 0)
                throw new ArgumentException("Shard number must be non-negative");

            return $"{database}${shardNumber}";
        }

        public static string ToDatabaseName(string shardName)
        {
            int shardNumberPosition = shardName.IndexOf('$');
            if (shardNumberPosition == -1)
                return shardName;

            var databaseName = shardName.Substring(0, shardNumberPosition);
            ResourceNameValidator.AssertValidDatabaseName(databaseName);

            return databaseName;
        }

        public static bool TryGetShardNumberAndDatabaseName(string databaseName, out string shardedDatabaseName, out int shardNumber)
        {
            var index = databaseName.IndexOf('$');
            shardNumber = -1;

            if (index != -1)
            {
                var slice = databaseName.AsSpan().Slice(index + 1);
                shardedDatabaseName = databaseName.Substring(0, index);
                if (int.TryParse(slice.ToString(), out shardNumber) == false)
                    throw new ArgumentException(nameof(shardedDatabaseName), "Unable to parse sharded database name: " + shardedDatabaseName);

                return true;
            }

            shardedDatabaseName = databaseName;
            return false;
        }

        public static int? GetShardNumberFromDatabaseName(string databaseName)
        {
            if (TryGetShardNumberAndDatabaseName(databaseName, out _, out var shardNumber))
                return shardNumber;

            return null;
        }

        public static bool IsShardName(string shardName)
        {
            return shardName.IndexOf('$') != -1;
        }

        public static int GetShardNumberFor(ShardingConfiguration configuration, string id)
        {
            var bucket = GetBucketFor(configuration, id);
            return FindBucketShard(configuration.BucketRanges, bucket);
        }

        public static int GetBucketFor(ShardingConfiguration configuration, string id)
        {
            var lowerId = Encodings.Utf8.GetBytes(id.ToLowerInvariant());
            return GetBucketFor(configuration, lowerId);
        }

        public static int GetBucketFor(ShardingConfiguration configuration, ReadOnlySpan<byte> lowerId)
        {
            var bucket = GetBucketFor(lowerId);

            if (configuration?.Prefixed != null)
            {
                foreach (var setting in configuration.Prefixed)
                {
                    if (lowerId.StartsWith(setting.PrefixBytesLowerCase))
                    {
                        bucket += setting.BucketRangeStart;
                        break;
                    }
                }
            }

            return bucket;
        }

        private static int GetBucketFor(ReadOnlySpan<byte> buffer)
        {
            var len = buffer.Length;
            for (int i = len - 1; i > 0; i--)
            {
                if (buffer[i] != (byte)'$')
                    continue;

                buffer = buffer.Slice(i + 1, len - i - 1);
                break;
            }

            var hash = Hashing.XXHash64.Calculate(buffer);
            return (int)(hash % NumberOfBuckets);
        }

        public static int FindBucketShard(List<ShardBucketRange> ranges, int bucket)
        {
            int prefixRange = bucket >> 20;
            for (int i = 0; i < ranges.Count - 1; i++)
            {
                int bucketRangeStart = ranges[i].BucketRangeStart;
                if ((bucketRangeStart >> 20) != prefixRange)
                    continue;

                int nextBucketRangeStart = ranges[i + 1].BucketRangeStart;
                if (bucket < nextBucketRangeStart)
                    return ranges[i].ShardNumber;
            }

            return ranges[ranges.Count - 1].ShardNumber;
        }
    }
}
