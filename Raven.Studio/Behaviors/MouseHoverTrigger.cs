using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Windows.Threading;

namespace Raven.Studio.Behaviors
{
    public class MouseHoverTrigger : TriggerBase<FrameworkElement>
    {
        private DispatcherTimer _timer;

        public TimeSpan HoverTime
        {
            get { return (TimeSpan)GetValue(HoverTimeProperty); }
            set { SetValue(HoverTimeProperty, value); }
        }

        public static readonly DependencyProperty HoverTimeProperty =
            DependencyProperty.Register("HoverTime", typeof(TimeSpan), typeof(MouseHoverTrigger), new PropertyMetadata(TimeSpan.FromMilliseconds(400)));

        
        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.MouseEnter += HandleMouseEnter;
            AssociatedObject.MouseLeave += HandleMouseLeave;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.MouseEnter -= HandleMouseEnter;
            AssociatedObject.MouseLeave -= HandleMouseLeave;
        }

        private void HandleMouseLeave(object sender, MouseEventArgs e)
        {
            _timer.Stop();
            _timer = null;
        }

        private void HandleMouseEnter(object sender, MouseEventArgs e)
        {
            _timer = new DispatcherTimer() { Interval = HoverTime};
            _timer.Tick += HandleTimerCompleted;
            _timer.Start();
        }

        private void HandleTimerCompleted(object sender, EventArgs e)
        {
            if (_timer != null)
            {
                _timer.Stop();
            }
            
            InvokeActions(null);
        }
    }
}
