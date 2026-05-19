using System;
using System.Collections.Generic;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public class StoreTaskItemErrorsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly TaskCategory _taskCategory;
    private readonly string _taskName;
    private readonly List<TaskItemError> _itemErrors;

    public StoreTaskItemErrorsCommand(TaskCategory taskCategory, string taskName, List<TaskItemError> itemErrors)
    {
        _taskCategory = taskCategory;
        _taskName = taskName;
        _itemErrors = itemErrors;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        context.DocumentDatabase.TaskErrorsStorage.StoreItemErrors(context, _taskCategory, _taskName, _itemErrors);
        return 1;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        throw new NotImplementedException();
    }
}
