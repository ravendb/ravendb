using System.Diagnostics;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Sharding;
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
            oldSetting.Shards = Setting.Shards;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Setting)] = Setting.ToJson();
        }
    }
}
