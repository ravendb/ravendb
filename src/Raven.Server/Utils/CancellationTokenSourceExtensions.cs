using System;
using System.Threading;
using Sparrow.Logging;

namespace Raven.Server.Utils;

internal static class CancellationTokenSourceExtensions
{
    /// <summary>
    /// Cancels the <see cref="CancellationTokenSource"/> and catches any <see cref="System.AggregateException"/>
    /// thrown by registered callbacks (e.g. stream.Dispose registered via token.Register in ParseToMemoryAsync).
    /// CancellationTokenSource.Cancel() wraps all callback exceptions in AggregateException — it is the only exception type it throws.
    /// Catching it allows the caller's cleanup to proceed regardless of broken-connection errors in callbacks.
    /// </summary>
    public static void SafeCancel(this CancellationTokenSource cts, Logger logger, string component)
    {
        try
        {
            cts.Cancel();
        }
        catch (AggregateException e)
        {
            if (logger.IsInfoEnabled)
                logger.Info($"Failed to cancel {nameof(CancellationTokenSource)} while disposing of {component}", e);
        }
    }
}
