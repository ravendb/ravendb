using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public class ClusterTransactionMergedCommandDto : TransactionOperationsMerger.IReplayableCommandDto<ClusterTransactionMergedCommand>
{
    public List<ClusterTransactionCommand.SingleClusterDatabaseCommand> Batch { get; set; }

    public ClusterTransactionMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        var command = new ClusterTransactionMergedCommand(database, new ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand>(Batch.ToArray()));
        return command;
    }
}
