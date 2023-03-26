using System.Collections.Generic;
using Raven.Server.ServerWide.Commands.Sharding;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public abstract class PutSubscriptionBatchCommandBase<T> : CommandBase
    where T : PutSubscriptionCommand
    {
        public List<T> Commands;

        public PutSubscriptionBatchCommandBase()
        {
        }

        protected PutSubscriptionBatchCommandBase(List<T> commands, string uniqueRequestId) : base(uniqueRequestId)
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

    public class PutSubscriptionBatchCommand : PutSubscriptionBatchCommandBase<PutSubscriptionCommand>
    {

        public PutSubscriptionBatchCommand()
        {
        }

        public PutSubscriptionBatchCommand(List<PutSubscriptionCommand> commands, string uniqueRequestId) : base(commands, uniqueRequestId)
        {
        }
    }

    public class PutShardedSubscriptionBatchCommand : PutSubscriptionBatchCommandBase<PutShardedSubscriptionCommand>
    {
        public PutShardedSubscriptionBatchCommand()
        {
        }

        public PutShardedSubscriptionBatchCommand(List<PutShardedSubscriptionCommand> commands, string uniqueRequestId) : base(commands, uniqueRequestId)
        {
            Commands = commands;
        }
    }
}
