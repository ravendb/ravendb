using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public class InitializeSchemaForNotificationsCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _tableName;

    public InitializeSchemaForNotificationsCommand(string tableName)
    {
        _tableName = tableName;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        Documents.Schemas.Notifications.Current.Create(context.Transaction.InnerTransaction, _tableName, 16);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}
