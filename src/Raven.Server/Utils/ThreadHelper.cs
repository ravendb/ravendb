using System;
using System.Threading;
using Sparrow.Logging;

namespace Raven.Server.Utils;

public static class ThreadHelper
{
    public static bool TrySetThreadPriority(ThreadPriority priority, string threadName, RavenLogger logger)
    {
        try
        {
            Thread.CurrentThread.Priority = priority;
            return true;
        }
        catch (Exception e)
        {
            if (logger.IsInfoEnabled && threadName != null)
            {
                logger.Info($"`{threadName}` was unable to set the thread priority to {priority}, will continue with the same priority", e);
            }
            return false;
        }
    }

    public static ThreadPriority GetThreadPriority()
    {
        try
        {
            return Thread.CurrentThread.Priority;
        }
        catch
        {
            return ThreadPriority.Normal;
        }
    }
}
