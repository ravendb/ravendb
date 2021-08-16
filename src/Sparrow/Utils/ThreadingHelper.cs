using System.Threading;

namespace Sparrow.Utils
{
    internal static class ThreadingHelper
    {
        public static bool InterlockedExchangeMax(ref long location, long newValue)
        {
            long initialValue;

            do
            {
                initialValue = location;
                if (initialValue >= newValue)
                    return false;
            } while (Interlocked.CompareExchange(ref location, newValue, initialValue) != initialValue);

            return true;
        }
    }
}
