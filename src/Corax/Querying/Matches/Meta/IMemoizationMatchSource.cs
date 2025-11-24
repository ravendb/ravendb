using System;

namespace Corax.Querying.Matches.Meta
{
    public interface IMemoizationMatchSource
    {
        MemoizationMatch Replay();
    }
}
