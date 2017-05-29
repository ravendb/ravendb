using System;
using System.Diagnostics;

namespace Raven.Server.Utils
{
    public static class MiscUtils
    {
        /// <summary>
        /// set longer timespan if debugging, so stuff won't timeout on breakpoints
        /// </summary>
        [Conditional("DEBUG")]
        public static void LongTimespanIfDebugging(ref TimeSpan timespan)
        {
            timespan = Debugger.IsAttached ? TimeSpan.FromHours(1) : timespan;
        }
    }
}
