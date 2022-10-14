using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.Utils;

public class ThreadGuardian
{
#if DEBUG
    private readonly int _threadId;

    private readonly string _threadName;
#endif

    public ThreadGuardian()
    {
#if DEBUG
        var currentThread = Thread.CurrentThread;
        _threadId = currentThread.ManagedThreadId;
        _threadName = currentThread.Name;
#endif
    }

    [Conditional("DEBUG")]
    public void Guard()
    {
#if DEBUG
        var currentThread = Thread.CurrentThread;
        var currentThreadId = currentThread.ManagedThreadId;
        if (currentThreadId != _threadId)
            throw new InvalidOperationException($"Thread with name '{_threadName}' ({_threadId}) was switched to thread '{currentThread.Name}' ({currentThreadId}).");
#endif
    }
}
