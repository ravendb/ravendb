using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class AddOrUpdateCompareExchangeBatchCommand : CommandBase
    {
        public List<AddOrUpdateCompareExchangeCommand> Commands;
        public List<RemoveCompareExchangeCommand> RemoveCommands;
        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public AddOrUpdateCompareExchangeBatchCommand()
        {
        }

        public AddOrUpdateCompareExchangeBatchCommand(List<AddOrUpdateCompareExchangeCommand> addCommands, JsonOperationContext contextToWriteResult, string guid) : base(guid)
        {
            Commands = addCommands;
            ContextToWriteResult = contextToWriteResult;
        }

        public AddOrUpdateCompareExchangeBatchCommand(List<RemoveCompareExchangeCommand> removeCommands, JsonOperationContext contextToWriteResult, string guid) : base(guid)
        {
            RemoveCommands = removeCommands;
            ContextToWriteResult = contextToWriteResult;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            if (Commands != null)
            {
                var dja = new DynamicJsonArray();
                foreach (var command in Commands)
                {
                    dja.Add(command.ToJson(context));
                }
                djv[nameof(Commands)] = dja;
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
