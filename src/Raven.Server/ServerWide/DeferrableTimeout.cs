using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;

namespace Raven.Server.ServerWide;

public class DeferrableTimeout
{
    public class Promise
    {
        private readonly TimeSpan _timeout;
        private TaskCompletionSource _scheduled;
        private readonly MultipleUseFlag _finished;
        private DateTime _completeAt;

        public Promise(TimeSpan timeout)
        {
            _timeout = timeout;
            _finished = new MultipleUseFlag(raised: true);
        }

        public Result ScheduleOrDefer(out Task task) => ScheduleOrDefer(_timeout, out task);

        private Result ScheduleOrDefer(TimeSpan deferBy, out Task task)
        {
            _completeAt = DateTime.UtcNow.Add(deferBy);
            var prev = Interlocked.CompareExchange(ref _scheduled, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), null);
            task = _scheduled!.Task;

            if (prev == null)
            {
                Promises.Enqueue(this);
                return Result.Scheduled;
            }

            return Result.Deferred;
        }

        public void Reset() => _finished.Raise();

        protected internal bool TryComplete(DateTime now)
        {
            if (_finished.IsRaised() == false)
                return false;

            if (now >= _completeAt)
            {
                _finished.Lower();
                Interlocked.Exchange(ref _scheduled, null).TrySetResult();
                return true;
            }

            return false;
        }

        public enum Result
        {
            Scheduled,
            Deferred
        }
    }

    private static readonly Timer QuarterSecondTimer = new Timer((_) =>
    {
        if (Promises.IsEmpty)
            return;

        var current = Interlocked.Exchange(ref Promises, new ConcurrentQueue<Promise>());
        var now = DateTime.UtcNow;

        while (current.TryDequeue(out var promise))
        {
            if (promise.TryComplete(now) == false)
                Promises!.Enqueue(promise);
        }
    }, state: null, TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(250));

    private static ConcurrentQueue<Promise> Promises = new ConcurrentQueue<Promise>();
}
