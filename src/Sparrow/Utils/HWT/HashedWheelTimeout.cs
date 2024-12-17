using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils.HWT
{
    internal sealed class HashedWheelTimeout : Timeout
    {
        internal const int ST_INIT = 0;
        internal const int ST_CANCELLED = 1;
        internal const int ST_EXPIRED = 2;

        private volatile int _state = ST_INIT;
        internal int State { get { return _state; } }

        internal readonly HashedWheelTimer _timer;
        private readonly TimerTask _task;
        internal readonly long _deadline;

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        internal long _remainingRounds;

        internal HashedWheelTimeout _next;
        internal HashedWheelTimeout _prev;

        // The bucket to which the timeout was added
        internal HashedWheelBucket _bucket;


        internal HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline)
        {
            this._timer = timer;
            this._task = task;
            this._deadline = deadline;
        }


        public Timer Timer { get { return _timer; } }

        public TimerTask TimerTask { get { return _task; } }

        public bool Expired { get { return _state == ST_EXPIRED; } }

        public bool Cancelled { get { return _state == ST_CANCELLED; } }

        bool CompareAndSetState(int expected, int state)
        {
            int originalState = Interlocked.CompareExchange(ref _state, state, expected);
            return originalState == expected;
        }

        public bool Cancel()
        {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!CompareAndSetState(ST_INIT, ST_CANCELLED))
            {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            _timer._cancelledTimeouts.Enqueue(this);
            return true;
        }

        internal void Remove()
        {
            HashedWheelBucket bucket = _bucket;
            if (bucket != null)
            {
                bucket.Remove(this);
            }
            else
            {
                _timer.DecreasePendingTimeouts();
            }
        }

        public void Expire()
        {
            if (!CompareAndSetState(ST_INIT, ST_EXPIRED))
            {
                return;
            }

            Task.Run(() => {
                _task.Run(this);
            });
        }
    }
}
