using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands.Subscriptions;

public abstract class PutSubscriptionBatchCommandBase<T> : CommandBase
    where T : PutSubscriptionCommand
{
    public List<T> Commands;

    protected PutSubscriptionBatchCommandBase()
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

    public override void AssertBeforeAppend(ClusterOperationContext context, ServerStore serverStore, long index)
    {
        var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);
        serverStore.Cluster.AssertSubscriptionsBatchLicenseLimits(serverStore, items, Commands, context);
    }
}
