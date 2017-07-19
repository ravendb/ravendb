using System;

namespace Raven.Client.Util
{
    public class RunningOn
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