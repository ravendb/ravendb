using System.Diagnostics;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class DeletePrefixedSettingCommand : UpdateDatabaseCommand
    {
        public string Prefix;

        public DeletePrefixedSettingCommand()
        {
        }

        public DeletePrefixedSettingCommand(string prefix, string database, string raftId) : base(database, raftId)
        {
            Prefix = prefix;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            int index = 0;
            PrefixedShardingSetting setting = null;
            for (; index < record.Sharding.Prefixed.Count; index++)
            {
                setting = record.Sharding.Prefixed[index];
                if (setting.Prefix == Prefix)
                    break;
            }

            Debug.Assert(setting != null);
            record.Sharding.Prefixed.RemoveAt(index);

            for (int i = 0; i < record.Sharding.BucketRanges.Count; i++)
            {
                var range = record.Sharding.BucketRanges[i];
                if (range.BucketRangeStart != setting.BucketRangeStart)
                    continue;

                record.Sharding.BucketRanges.RemoveRange(i, setting.Shards.Count);
                break;
            }

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Prefix)] = Prefix;
        }
    }
}
