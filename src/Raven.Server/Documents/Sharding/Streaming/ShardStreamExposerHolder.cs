using System;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Streaming;

internal class ShardStreamExposerHolder<T> :  IAsyncDisposable where T : StreamExposerContent
{
    public T ContentExposer { get; set; }

    public IDisposable ContextReturn { get; set; }

    public ValueTask DisposeAsync()
    {
        var ea = new ExceptionAggregator($"Failed to dispose {nameof(ShardStreamExposerHolder<StreamExposerContent>)}");

        ea.Execute(() => ContentExposer.Complete());
        ea.Execute(ContextReturn);

        ea.ThrowIfNeeded();

        return ValueTask.CompletedTask;
    }
}
