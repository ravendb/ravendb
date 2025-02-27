using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.Embeddings;

public sealed class EmbeddingsBatchRequest : IDisposable
{
    public string Value { get; }
    public TaskCompletionSource<ReadOnlyMemory<float>> TaskCompletionSource { get; }
    private readonly CancellationTokenSource _linkedTokenSource;
    private readonly CancellationTokenRegistration _tokenRegistration;

    public EmbeddingsBatchRequest(string value, CancellationToken callerToken, CancellationToken workerToken)
    {
        Value = value;
        TaskCompletionSource = new TaskCompletionSource<ReadOnlyMemory<float>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(callerToken, workerToken);
        _tokenRegistration = _linkedTokenSource.Token.Register(() => TaskCompletionSource.TrySetCanceled(_linkedTokenSource.Token));
    }

    public void Dispose()
    {
        _tokenRegistration.Dispose();
        _linkedTokenSource.Dispose();
    }
}
