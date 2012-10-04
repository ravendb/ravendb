using System;
using System.Windows.Threading;

namespace Raven.Studio.Infrastructure
{
    public class DeferredActionInvoker
    {
        private readonly Action _action;
        private readonly TimeSpan _interval;
        private DispatcherTimer _timer;

        public DeferredActionInvoker(Action action, TimeSpan interval)
        {
            _action = action;
            _interval = interval;
        }

        public void Request()
        {
            if (_timer == null)
            {
                _timer = new DispatcherTimer() {Interval = _interval};
                _timer.Tick += delegate
                                   {
                                       _timer.Stop();
                                       _action();
                                   };
            }

            _timer.Stop();
            _timer.Start();
        }

        public void Cancel()
        {
            if (_timer != null)
            {
                _timer.Stop();
            }
        }
    }
}