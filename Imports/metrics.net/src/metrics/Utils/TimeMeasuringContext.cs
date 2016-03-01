using System;

namespace Metrics.Utils
{
    public struct TimeMeasuringContext
    {
        private readonly long start;
        private readonly Action<long> action;
        private bool disposed;

        public TimeMeasuringContext(Action<long> disposeAction)
        {
            this.start = Clock.Nanoseconds;
            this.action = disposeAction;
            this.disposed = false;
        }

        public TimeSpan Elapsed
        {
            get
            {
                var miliseconds = (Clock.Nanoseconds - this.start)/Clock.NANOSECONDS_IN_MILISECOND;
                return TimeSpan.FromMilliseconds(miliseconds);
            }
        }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }
            this.disposed = true;
            this.action(Clock.Nanoseconds - this.start);
        }
    }
}
