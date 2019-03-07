using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class AddOrUpdateCompareExchangeBatchCommand : CommandBase
    {
        public List<AddOrUpdateCompareExchangeCommand> AddOrUpdateCommands;
        public List<RemoveCompareExchangeCommand> RemoveCommands;
        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public AddOrUpdateCompareExchangeBatchCommand()
        {
        }

        public AddOrUpdateCompareExchangeBatchCommand(List<AddOrUpdateCompareExchangeCommand> commands, JsonOperationContext contextToWriteResult)
        {
            AddOrUpdateCommands = commands;
            ContextToWriteResult = contextToWriteResult;
        }

        public AddOrUpdateCompareExchangeBatchCommand(List<RemoveCompareExchangeCommand> commands, JsonOperationContext contextToWriteResult)
        {
            RemoveCommands = commands;
            ContextToWriteResult = contextToWriteResult;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            if (AddOrUpdateCommands != null)
            {
                var dja = new DynamicJsonArray();
                foreach (var command in AddOrUpdateCommands)
                {
                    dja.Add(command.ToJson(context));
                }
                djv[nameof(AddOrUpdateCommands)] = dja;
            }

            if (RemoveCommands != null)
            {
                var dja2 = new DynamicJsonArray();
                foreach (var command in RemoveCommands)
                {
                    dja2.Add(command.ToJson(context));
                }

                djv[nameof(RemoveCommands)] = dja2;
            }

            return djv;
        }
    }
}
