using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    public sealed class DeletePrefixedShardingSettingOperation : IMaintenanceOperation
    {
        private readonly string _prefix;

        public DeletePrefixedShardingSettingOperation(string prefix)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentNullException(nameof(prefix));

            _prefix = prefix;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new DeletePrefixedShardingSettingCommand(conventions, new PrefixedShardingSetting
            {
                Prefix = _prefix
            });
        }

        private sealed class DeletePrefixedShardingSettingCommand : PrefixedShardingCommand
        {
            protected override PrefixedCommandType CommandType => PrefixedCommandType.Delete;

            public DeletePrefixedShardingSettingCommand(DocumentConventions conventions, PrefixedShardingSetting setting) : base(conventions, setting)
            {
            }
        }
    }
}
