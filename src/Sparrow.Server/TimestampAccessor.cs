using System;
using System.Threading;

namespace Sparrow.Server
{
    // The idea behind the timestamp accessor is to be able to query the UTC datetime value
    // while at the same time avoid multiple calls to the kernel operations when they are not
    // needed for accuracy. 
    public static class TimestampAccessor
    {
        private static long _timestamp;
        private static readonly Timer _timer;

        // Static constructor to initialize the timestamp and timer
        static TimestampAccessor()
        {
            _timestamp = DateTime.UtcNow.Ticks;
            _timer = new Timer(UpdateTimestamp, null, 0, 500); // Update every 500 milliseconds (0.5 second)
        }

        // Method called by the timer to update the timestamp
        private static void UpdateTimestamp(object state)
        {
            // This will force an update.
            GetTimestamp();
        }

        public static long GetTimestamp()
        {
            long newTimestamp = DateTime.UtcNow.Ticks;
            long oldTimestamp = _timestamp;
            
            // We will bail if the new timestamp is smaller than or equal to the current one, no need to use
            // an older timestamp because of losing the CPU against someone else.
            if (newTimestamp <= oldTimestamp)
                return oldTimestamp;

            // Attempt to update the timestamp atomically, if it fails we just take the current one.
            Interlocked.CompareExchange(ref _timestamp, newTimestamp, oldTimestamp);

            return newTimestamp;
        }

        public static DateTime GetTime()
        {
            return new DateTime(GetTimestamp());
        }

        public static DateTime GetApproximateTime()
        {
            return new DateTime(GetApproximateTimestamp());
        }

        public static long GetApproximateTimestamp()
        {
            // We increase to ensure that we always get a new one even if approximate since by the time that
            // interlocked ended we have already elapsed a tick anyway.
            return Interlocked.Increment(ref _timestamp);
        }
    }
}
