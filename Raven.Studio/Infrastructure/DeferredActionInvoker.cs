using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
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
