using System;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    internal static class PowerBIRecognizerLog
    {
        // Parser-driven recognizers reject by throwing - normal "not this shape, fall through",
        // logged at Debug. NRE/bad-cast/bad-index aren't rejections but latent bugs that would
        // otherwise hide behind a generic "Unhandled query" - surface those at Info with the stack.
        public static void Rejected(RavenLogger logger, string context, Exception e)
        {
            if (e is NullReferenceException or InvalidCastException or IndexOutOfRangeException)
            {
                if (logger.IsInfoEnabled)
                    logger.Info(context, e);
                return;
            }

            if (logger.IsDebugEnabled)
                logger.Debug($"{context} Reason: {e.Message}");
        }
    }
}
