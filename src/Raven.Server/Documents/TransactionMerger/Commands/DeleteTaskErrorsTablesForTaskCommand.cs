using System;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class DeleteTaskErrorsTablesForTaskCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly string _taskName;
    private readonly TaskCategory _taskCategory;

    public DeleteTaskErrorsTablesForTaskCommand(string taskName, TaskCategory taskCategory)
    {
        _taskName = taskName;
        _taskCategory = taskCategory;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        context.DocumentDatabase.TaskErrorsStorage.DeleteTaskErrorsTablesForTask(context, _taskCategory, _taskName);
        return 1;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        throw new NotImplementedException();
    }
}
