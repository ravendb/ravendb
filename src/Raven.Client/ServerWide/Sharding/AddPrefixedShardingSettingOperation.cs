using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    public sealed class AddPrefixedShardingSettingOperation : IMaintenanceOperation
    {
        private readonly PrefixedShardingSetting _setting;

        public AddPrefixedShardingSettingOperation(PrefixedShardingSetting setting)
        {
            _setting = setting ?? throw new ArgumentNullException(nameof(setting));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new AddPrefixedShardingSettingCommand(conventions, _setting);
        }

        private sealed class AddPrefixedShardingSettingCommand : PrefixedShardingCommand
        {
            protected override PrefixedCommandType CommandType => PrefixedCommandType.Add;

            public AddPrefixedShardingSettingCommand(DocumentConventions conventions, PrefixedShardingSetting setting) : base(conventions, setting)
            {
            }
        }
    }
}
