using System.Threading;

namespace Raven.Server.Documents.Changes;

public static class ChangesClientConnectionId
{
    private static long Counter;

    public static long GetNextId()
    {
        return Interlocked.Increment(ref Counter);
    }
}
