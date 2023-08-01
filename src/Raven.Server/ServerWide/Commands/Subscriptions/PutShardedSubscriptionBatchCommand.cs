using System.Collections.Generic;
using Raven.Server.ServerWide.Commands.Sharding;

namespace Raven.Server.ServerWide.Commands.Subscriptions;

public sealed class PutShardedSubscriptionBatchCommand : PutSubscriptionBatchCommandBase<PutShardedSubscriptionCommand>
{
    public PutShardedSubscriptionBatchCommand()
    {
    }

    public PutShardedSubscriptionBatchCommand(List<PutShardedSubscriptionCommand> commands, string uniqueRequestId) : base(commands, uniqueRequestId)
    {
        Commands = commands;
    }
}
