using System;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents;

internal sealed class BulkOperationCommand<T> : DocumentMergedTransactionCommand where T : DocumentMergedTransactionCommand
{
    private readonly T _command;
    private readonly Func<T, IBulkOperationDetails> _getDetails;
    private readonly Action<T> _afterExecuted;

    public BulkOperationCommand(T command, Func<T, IBulkOperationDetails> getDetails, Action<T> afterExecuted)
    {
        _command = command;
        _getDetails = getDetails;
        _afterExecuted = afterExecuted;
    }

    public override long Execute(DocumentsOperationContext context, AbstractTransactionOperationsMerger<DocumentsOperationContext, DocumentsTransaction>.RecordingState recording)
    {
        try
        {
            var count = _command.Execute(context, recording);
            if (_getDetails != null)
                RetrieveDetails?.Invoke(_getDetails(_command));

            return count;
        }
        finally
        {
            _afterExecuted?.Invoke(_command);
        }
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        throw new NotSupportedException($"ToDto() of {nameof(BulkOperationCommand<T>)} Should not be called");
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        throw new NotSupportedException("Should only call Execute() here");
    }

    public Action<IBulkOperationDetails> RetrieveDetails { private get; set; }
}
