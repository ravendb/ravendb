using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class AddPrefixedSettingCommand : UpdateDatabaseCommand
    {
        public PrefixedShardingSetting Setting;

        public AddPrefixedSettingCommand()
        {
        }

        public AddPrefixedSettingCommand(PrefixedShardingSetting setting, string database, string raftId) : base(database, raftId)
        {
            Setting = setting;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var maxBucketRangeStart = 0;
            record.Sharding.Prefixed ??= new List<PrefixedShardingSetting>();

            foreach (var value in record.Sharding.Prefixed)
            {
                if (maxBucketRangeStart < value.BucketRangeStart)
                    maxBucketRangeStart = value.BucketRangeStart;
            }


            Setting.BucketRangeStart = maxBucketRangeStart + ShardHelper.NumberOfBuckets;

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

            record.Sharding.Prefixed.Add(Setting);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Setting)] = Setting.ToJson();
        }
    }
}
