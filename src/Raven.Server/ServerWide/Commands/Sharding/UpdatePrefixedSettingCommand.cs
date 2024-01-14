using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class UpdatePrefixedSettingCommand : UpdateDatabaseCommand
    {
        public PrefixedShardingSetting Setting;

        public UpdatePrefixedSettingCommand()
        {
        }

        public UpdatePrefixedSettingCommand(PrefixedShardingSetting setting, string database, string raftId) : base(database, raftId)
        {
            Setting = setting;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var location = record.Sharding.Prefixed.BinarySearch(Setting, PrefixedSettingComparer.Instance);
            Debug.Assert(location >= 0, $"Prefixed setting '{Setting.Prefix}' was not found in sharding configuration");

            var oldSetting = record.Sharding.Prefixed[location];
            int index = 0;
            for (; index < record.Sharding.BucketRanges.Count; index++)
            {
                if (record.Sharding.BucketRanges[index].BucketRangeStart != oldSetting.BucketRangeStart)
                    continue;

                record.Sharding.BucketRanges.RemoveRange(index, oldSetting.Shards.Count);
                break;
            }

            var remainingShards = Setting.Shards
                .Where(shard => oldSetting.Shards.Contains(shard))
                .OrderBy(x => x)
                .ToList();

            var newRanges = new List<ShardBucketRange>();
            var rangeStart = oldSetting.BucketRangeStart;
            var step = ShardHelper.NumberOfBuckets / remainingShards.Count;

            foreach (var shardNumber in remainingShards)
            {
                newRanges.Add(new ShardBucketRange
                {
                    ShardNumber = shardNumber,
                    BucketRangeStart = rangeStart
                });
                rangeStart += step;
            }

            record.Sharding.BucketRanges.InsertRange(index, newRanges);

            oldSetting.Shards = Setting.Shards;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Setting)] = Setting.ToJson();
        }
    }
}
