using System;

namespace Corax.Queries
{
    public interface IMemoizationMatchSource : IDisposable
    {
        bool IsOrdered { get; }

        MemoizationMatch Replay();
    }
}
