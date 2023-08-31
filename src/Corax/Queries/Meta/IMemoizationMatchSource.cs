using System;

namespace Corax.Queries.Meta
{
    public interface IMemoizationMatchSource : IDisposable
    {
        MemoizationMatch Replay();
    }
}
