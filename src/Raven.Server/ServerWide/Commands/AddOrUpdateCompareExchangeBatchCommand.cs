using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class AddOrUpdateCompareExchangeBatchCommand : CommandBase
    {
        public List<AddOrUpdateCompareExchangeCommand> Commands;

        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public AddOrUpdateCompareExchangeBatchCommand()
        {
        }

        public AddOrUpdateCompareExchangeBatchCommand(List<AddOrUpdateCompareExchangeCommand> commands, JsonOperationContext contextToWriteResult)
        {
            Commands = commands;
            ContextToWriteResult = contextToWriteResult;
        }
        
        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            var dja = new DynamicJsonArray();
            foreach (var command in Commands)
            {
                dja.Add(command.ToJson(context));
            }
            djv[nameof(Commands)] = dja;

            return djv;
        }
    }
}
