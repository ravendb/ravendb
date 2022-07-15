using System;

namespace Corax.Queries
{
    public interface IMemoizationMatchSource : IDisposable
    {
        MemoizationMatch Replay();
    }
}
