using System;
using Raven.Client.Documents.BulkInsert;

namespace Raven.Client.Documents.Operations;

internal class BulkInsertObserver : IObserver<OperationStatusChange>
{
    private readonly BulkInsertOperation _operation;
    
    internal BulkInsertObserver(BulkInsertOperation operation)
    {
        _operation = operation;
    }
    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(OperationStatusChange value)
    {
        if (value?.State?.Status == OperationStatus.InProgress)
            _operation.InvokeOnProgress(value.State.Progress as BulkInsertProgress);
    }
}
