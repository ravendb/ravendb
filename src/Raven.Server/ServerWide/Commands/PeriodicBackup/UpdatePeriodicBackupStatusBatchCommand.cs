using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public sealed class UpdatePeriodicBackupStatusBatchCommand : CommandBase
    {
        public List<UpdatePeriodicBackupStatusCommand> Commands;

        public UpdatePeriodicBackupStatusBatchCommand()
        {
        }

        public UpdatePeriodicBackupStatusBatchCommand(List<UpdatePeriodicBackupStatusCommand> commands, string uniqueRequestId) : base(uniqueRequestId)
        {
            Commands = commands;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            var commands = new DynamicJsonArray();
            foreach (var command in Commands)
                commands.Add(command.ToJson(context));

            djv[nameof(Commands)] = commands;

            return djv;
        }
    }
}
