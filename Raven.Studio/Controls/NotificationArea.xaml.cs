using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Windows.Threading;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Controls
{
    public partial class NotificationArea : UserControl
    {
        public static readonly DependencyProperty NotificationsProperty =
            DependencyProperty.Register("Notifications", typeof (BindableCollection<Notification>), typeof (NotificationArea), new PropertyMetadata(default(BindableCollection<Notification>), HandleNotificationsCollectionChanged));

        private NotificationView _currentNotification;
        private DispatcherTimer _timer;

        public BindableCollection<Notification> Notifications
        {
            get { return (BindableCollection<Notification>) GetValue(NotificationsProperty); }
            set { SetValue(NotificationsProperty, value); }
        }

        public NotificationArea()
        {
            InitializeComponent();

            _timer = new DispatcherTimer() {Interval = TimeSpan.FromSeconds(3.0)};
            _timer.Tick += delegate { if (_currentNotification != null) RemoveOldNotification(); };
        }

        private static void HandleNotificationsCollectionChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var area = d as NotificationArea;

            if (e.OldValue != null)
            {
                var collection = e.OldValue as BindableCollection<Notification>;
                collection.CollectionChanged -= area.HandleCollectionChanged;
            }

            if (e.NewValue != null)
            {
                var collection = e.NewValue as BindableCollection<Notification>;
                collection.CollectionChanged += area.HandleCollectionChanged;
            }
        }

        private void HandleCollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            if (e.Action == NotifyCollectionChangedAction.Add)
            {
                var replaceExisting = false;
                var minWidth = 0.0;

                if (_currentNotification != null)
                {
                    minWidth = _currentNotification.ActualWidth;
                    replaceExisting = true;
                    RemoveOldNotification();
                }

                _currentNotification = new NotificationView() { DataContext = e.NewItems[0], MinWidth = minWidth};
                _currentNotification.MouseLeftButtonUp +=
                    delegate { if (_currentNotification != null) RemoveOldNotification(); };

                LayoutRoot.Children.Add(_currentNotification);
                _currentNotification.Display(replaceExisting);
                _timer.Start();
            }
        }

        private void RemoveOldNotification()
        {
            var oldView = _currentNotification;

            oldView.Hide(() => LayoutRoot.Children.Remove(oldView));

            _currentNotification = null;
            _timer.Stop();
        }
    }
}
