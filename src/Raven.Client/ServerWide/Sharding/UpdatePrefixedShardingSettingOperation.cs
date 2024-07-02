using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    public sealed class UpdatePrefixedShardingSettingOperation : IMaintenanceOperation
    {
        private readonly PrefixedShardingSetting _setting;

        public UpdatePrefixedShardingSettingOperation(PrefixedShardingSetting setting)
        {
            _setting = setting ?? throw new ArgumentNullException(nameof(setting));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePrefixedShardingSettingCommand(conventions, _setting);
        }

        private sealed class UpdatePrefixedShardingSettingCommand : PrefixedShardingCommand
        {
            public UpdatePrefixedShardingSettingCommand(DocumentConventions conventions, PrefixedShardingSetting setting) : base(conventions, setting)
            {
            }

            protected override PrefixedCommandType CommandType => PrefixedCommandType.Update;

        }
    }
}
