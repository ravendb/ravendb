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

namespace Raven.Studio.Infrastructure
{
    /// <summary>
    /// Ensures that a method doesn't get called more often than the minimum interval
    /// </summary>
    public class SamplingInvoker
    {
        private readonly TimeSpan minimumInterval;
        private DateTime lastInvoked = DateTime.MinValue;

        public SamplingInvoker(TimeSpan minimumInterval)
        {
            this.minimumInterval = minimumInterval;
        }

        public void ResetInterval()
        {
            lastInvoked = DateTime.MinValue;
        }

        public void TryInvoke(Action action)
        {
            if (DateTime.Now - lastInvoked >= minimumInterval)
            {
                try
                {
                    action();
                }
                finally
                {
                    lastInvoked = DateTime.Now;
                }
            }
        }
    }
}
