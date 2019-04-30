using System.Collections.Generic;
using System.IO;
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
                //precaution, prevent invalid subscription batch commands being sent.
                if(command.SubscriptionId == null && string.IsNullOrEmpty(command.SubscriptionName))
                    throw new InvalidDataException($"Invalid {nameof(PutSubscriptionCommand)}: {nameof(PutSubscriptionCommand.SubscriptionId)} or {nameof(PutSubscriptionCommand.SubscriptionName)} must not be empty. This should not happen and is likely due to a bug.");
                dja.Add(command.ToJson(context));
            }
            djv[nameof(Commands)] = dja;

            return djv;
        }
    }
}
