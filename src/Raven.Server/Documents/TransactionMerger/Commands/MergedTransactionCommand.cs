using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public abstract class DocumentMergedTransactionCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{

}

public abstract class MergedTransactionCommand<TOperationContext, TTransaction> : IRecordableCommand<TOperationContext, TTransaction>
    where TOperationContext : TransactionOperationContext<TTransaction>
    where TTransaction : RavenTransaction
{
    public bool UpdateAccessTime = true;

    protected abstract long ExecuteCmd(TOperationContext context);

    internal long ExecuteDirectly(TOperationContext context)
    {
        return ExecuteCmd(context);
    }

    public virtual long Execute(TOperationContext context, AbstractTransactionOperationsMerger<TOperationContext, TTransaction>.RecordingState recordingState)
    {
        recordingState?.TryRecord(context, this);

        return ExecuteCmd(context);
    }

    public abstract IReplayableCommandDto<TOperationContext, TTransaction, MergedTransactionCommand<TOperationContext, TTransaction>> ToDto(TOperationContext context);

    [JsonIgnore]
    public readonly TaskCompletionSource<object> TaskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Exception Exception;

    public bool RetryOnError = false;
}

