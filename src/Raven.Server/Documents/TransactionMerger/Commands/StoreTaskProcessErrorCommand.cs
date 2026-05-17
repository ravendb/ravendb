using System;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class StoreTaskProcessErrorCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly TaskCategory _taskCategory;
    private readonly TaskProcessError _processError;

    public StoreTaskProcessErrorCommand(TaskCategory taskCategory, TaskProcessError processError)
    {
        _taskCategory = taskCategory;
        _processError = processError;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        context.DocumentDatabase.TaskErrorsStorage.StoreProcessError(context, _taskCategory, _processError);
        return 1;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        throw new NotImplementedException();
    }
}
