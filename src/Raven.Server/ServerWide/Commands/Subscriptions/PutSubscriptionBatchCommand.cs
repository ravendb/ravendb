using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class PutSubscriptionBatchCommand : CommandBase
    {
        public List<PutSubscriptionCommand> Commands;

        public PutSubscriptionBatchCommand()
        {
        }

        public PutSubscriptionBatchCommand(List<PutSubscriptionCommand> commands)
        {
            Commands = commands;
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