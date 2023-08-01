using System;

namespace Raven.Client.Util
{
    internal sealed class RunningOn
    {
        [ThreadStatic]
        public static bool FinalizerThread;

        static RunningOn()
        {
            GC.KeepAlive(new RunningOn());
        }

        ~RunningOn()
        {
            FinalizerThread = true;
        }
    }
}
