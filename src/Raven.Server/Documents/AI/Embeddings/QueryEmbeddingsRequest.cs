using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.Embeddings;

public sealed class QueryEmbeddingsRequest : IDisposable
{
    private readonly CancellationTokenRegistration _tokenRegistration;

    public IList<string> Values { get; }

    public TaskCompletionSource<ReadOnlyMemory<float>[]> TaskCompletionSource { get; }

    public QueryEmbeddingsRequest(IList<string> values, CancellationToken callerToken)
    {
        Values = values;
        TaskCompletionSource = new TaskCompletionSource<ReadOnlyMemory<float>[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (callerToken.CanBeCanceled)
            _tokenRegistration = callerToken.Register(() => {
                TaskCompletionSource.TrySetCanceled(callerToken);
            });
    }

    public void Dispose()
    {
        _tokenRegistration.Dispose();
    }
}
