using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class AddPrefixedShardingSettingCommand : UpdateDatabaseCommand
    {
        public PrefixedShardingSetting Setting;

        public AddPrefixedShardingSettingCommand()
        {
        }

        public AddPrefixedShardingSettingCommand(PrefixedShardingSetting setting, string database, string raftId) : base(database, raftId)
        {
            Setting = setting;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Sharding.Prefixed ??= new List<PrefixedShardingSetting>();
            Setting.BucketRangeStart = GetNextPrefixedBucketRangeStart(record.Sharding.Prefixed);

            var rangeStart = Setting.BucketRangeStart;
            var shards = Setting.Shards;
            var step = ShardHelper.NumberOfBuckets / shards.Count;

            foreach (var shardNumber in shards.OrderBy(x => x))
            {
                record.Sharding.BucketRanges.Add(new ShardBucketRange
                {
                    ShardNumber = shardNumber,
                    BucketRangeStart = rangeStart
                });
                rangeStart += step;
            }

            var index = record.Sharding.Prefixed.BinarySearch(Setting, PrefixedSettingComparer.Instance);
            record.Sharding.Prefixed.Insert(~index, Setting);
        }

        private static int GetNextPrefixedBucketRangeStart(IEnumerable<PrefixedShardingSetting> prefixes)
        {
            var prefixesByRangeStart = prefixes.OrderBy(x => x.BucketRangeStart).ToList();
            if (prefixesByRangeStart.Count == 0)
                return ShardHelper.NumberOfBuckets;

            for (int i = 0; i < prefixesByRangeStart.Count; i++)
            {
                var currentRangeStart = prefixesByRangeStart[i].BucketRangeStart;
                var nextRangeStart = i + 1 < prefixesByRangeStart.Count 
                    ? prefixesByRangeStart[i + 1].BucketRangeStart 
                    : int.MaxValue;

                var expectedNext = currentRangeStart + ShardHelper.NumberOfBuckets;
                if (expectedNext < nextRangeStart)
                {
                    // found gap
                    return expectedNext;
                }
            }

            return prefixesByRangeStart[^1].BucketRangeStart + ShardHelper.NumberOfBuckets;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Setting)] = Setting.ToJson();
        }
    }
}
