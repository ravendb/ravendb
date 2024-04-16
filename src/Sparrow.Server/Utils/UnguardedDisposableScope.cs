using System;
using System.Collections.Generic;

namespace Sparrow.Server.Utils;

public sealed class UnguardedDisposableScope : IDisposable
{
    private readonly LinkedList<IDisposable> _disposables = new();
    private int _delayedDispose;

    public void EnsureDispose(IDisposable toDispose)
    {
        _disposables.AddFirst(toDispose);
    }

    public void Dispose()
    {
        if (_delayedDispose-- > 0)
            return;

        foreach (var disposable in _disposables)
            disposable.Dispose();
    }

    public IDisposable Delay()
    {
        _delayedDispose++;

        return this;
    }
}
