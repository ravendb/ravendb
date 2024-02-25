using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;  
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class DeletePrefixedShardingSettingCommand : UpdateDatabaseCommand
    {
        public PrefixedShardingSetting Setting;

        public DeletePrefixedShardingSettingCommand()
        {
        }

        public DeletePrefixedShardingSettingCommand(PrefixedShardingSetting setting, string database, string raftId) : base(database, raftId)
        {
            Setting = setting;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            int prefixIndex = record.Sharding.Prefixed.BinarySearch(Setting, PrefixedSettingComparer.Instance);
            if (prefixIndex < 0)
                throw new RachisApplyException($"Prefixed setting '{Setting.Prefix}' was not found in sharding configuration");

            var prefixRangeStart = record.Sharding.Prefixed[prefixIndex].BucketRangeStart;
            var nextPrefixRangeStart = prefixRangeStart + ShardHelper.NumberOfBuckets;

            record.Sharding.Prefixed.RemoveAt(prefixIndex);

            var numberOfRangesToRemove = 0;
            var firstRangeIndex = 0;
            for (int i = 0; i < record.Sharding.BucketRanges.Count; i++)
            {
                var range = record.Sharding.BucketRanges[i];
                if (range.BucketRangeStart < prefixRangeStart)
                    continue;

                if (range.BucketRangeStart == prefixRangeStart)
                    firstRangeIndex = i;

                else if (range.BucketRangeStart >= nextPrefixRangeStart)
                    break;

                numberOfRangesToRemove++;
            }

            record.Sharding.BucketRanges.RemoveRange(firstRangeIndex, numberOfRangesToRemove);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Setting)] = Setting.ToJson();
        }
    }
}
