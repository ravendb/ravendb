using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.Embeddings;

public sealed class EmbeddingsBatchRequest : IDisposable
{
    public IList<string> Values { get; }
    public TaskCompletionSource<IList<ReadOnlyMemory<float>>> TaskCompletionSource { get; }
    private readonly CancellationTokenSource _linkedTokenSource;
    private readonly CancellationTokenRegistration _tokenRegistration;

    public EmbeddingsBatchRequest(IList<string> values, CancellationToken callerToken, CancellationToken workerToken)
    {
        Values = values;
        TaskCompletionSource = new TaskCompletionSource<IList<ReadOnlyMemory<float>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(callerToken, workerToken);
        _tokenRegistration = _linkedTokenSource.Token.Register(() => TaskCompletionSource.TrySetCanceled(_linkedTokenSource.Token));
    }

    public void Dispose()
    {
        _tokenRegistration.Dispose();
        _linkedTokenSource.Dispose();
    }
}
