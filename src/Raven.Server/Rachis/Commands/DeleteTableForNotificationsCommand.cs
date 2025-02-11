using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public class DeleteTableForNotificationsCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _tableName;

    public DeleteTableForNotificationsCommand(string tableName)
    {
        _tableName = tableName;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        context.Transaction.InnerTransaction.DeleteTable(_tableName);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}
