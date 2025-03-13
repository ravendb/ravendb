using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.Embeddings;

public sealed class QueryEmbeddingsBatchRequest : IDisposable
{
    public IList<string> Values { get; }
    public TaskCompletionSource<ReadOnlyMemory<float>[]> TaskCompletionSource { get; }
    private readonly CancellationTokenSource _linkedTokenSource;
    private readonly CancellationTokenRegistration _tokenRegistration;

    public QueryEmbeddingsBatchRequest(IList<string> values, CancellationToken callerToken, CancellationToken workerToken)
    {
        Values = values;
        TaskCompletionSource = new TaskCompletionSource<ReadOnlyMemory<float>[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(callerToken, workerToken);
        _tokenRegistration = _linkedTokenSource.Token.Register(() => TaskCompletionSource.TrySetCanceled(_linkedTokenSource.Token));
    }

    public Task<ReadOnlyMemory<float>[]> CancelWithShutdownMessage()
    {
        TaskCompletionSource.TrySetException(new OperationCanceledException(QueryEmbeddingsBatchingService.ShutdownMessage));
        return TaskCompletionSource.Task;
    }

    public void Dispose()
    {
        _tokenRegistration.Dispose();
        _linkedTokenSource.Dispose();
    }
}
