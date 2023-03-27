using System.Collections.Generic;
using Raven.Server.ServerWide.Commands.Sharding;

namespace Raven.Server.ServerWide.Commands.Subscriptions;

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
