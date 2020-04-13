using System;
using System.Diagnostics;

namespace Raven.Server.Utils
{
    public static class DebuggerAttachedTimeout
    {
        public static bool DisableLongTimespan = true;

        private static bool IsDisabled => DisableLongTimespan ||
                                          Debugger.IsAttached == false;

        [Conditional("DEBUG")]
        public static void OutgoingReplication(ref int timespan)
        {
            if (IsDisabled)
                return;

            timespan *= 10;
        }

        [Conditional("DEBUG")]
        public static void SendTimeout(ref int timespan)
        {
            if (IsDisabled)
                return;

            timespan *= 100;
        }

        [Conditional("DEBUG")]
        public static void LongTimespanIfDebugging(ref TimeSpan timespan)
        {
            if (IsDisabled)
                return;

            timespan = TimeSpan.FromMinutes(1);
        }
    }
}
