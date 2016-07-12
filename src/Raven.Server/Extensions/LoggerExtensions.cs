using System;
using System.Runtime.CompilerServices;

namespace Raven.Server
{
    public static class LoggerExtensions
    {
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
	    public static void InfoIfEnabled(this Sparrow.Logging.Logger log, string message, Exception ex = null)
	    {
		    if (log.IsInfoEnabled)
			    log.Info(message, ex);
	    }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void OperaitonsIfEnabled(this Sparrow.Logging.Logger log, string message, Exception ex = null)
		{
			if (log.IsOperationsEnabled)
				log.Operations(message, ex);
		}
	}
}
