using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveCompareExchangeBatchCommand : CommandBase
    {
        public List<RemoveCompareExchangeCommand> Commands;
        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public RemoveCompareExchangeBatchCommand()
        {
            // for deserialization
        }

        public RemoveCompareExchangeBatchCommand(List<RemoveCompareExchangeCommand> commands, JsonOperationContext contextToWriteResult)
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
