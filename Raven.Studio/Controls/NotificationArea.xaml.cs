using System;
using System.Collections.Specialized;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Controls
{
    public partial class NotificationArea : UserControl
    {
        public static readonly DependencyProperty NotificationsProperty =
            DependencyProperty.Register("Notifications", typeof (BindableCollection<Notification>), typeof (NotificationArea), new PropertyMetadata(default(BindableCollection<Notification>), HandleNotificationsCollectionChanged));

        private NotificationView currentNotification;
        private readonly DispatcherTimer timer;

        public BindableCollection<Notification> Notifications
        {
            get { return (BindableCollection<Notification>) GetValue(NotificationsProperty); }
            set { SetValue(NotificationsProperty, value); }
        }

        public NotificationArea()
        {
            InitializeComponent();

            timer = new DispatcherTimer() {Interval = TimeSpan.FromSeconds(3.0)};
            timer.Tick += delegate { if (currentNotification != null) RemoveOldNotification(); };
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

                if (currentNotification != null)
                {
                    minWidth = currentNotification.ActualWidth;
                    replaceExisting = true;
                    RemoveOldNotification();
                }

                var item = e.NewItems[0] as Notification;

                currentNotification = new NotificationView() { DataContext = item, MinWidth = minWidth};
                currentNotification.MouseLeftButtonUp +=
                    delegate { if (currentNotification != null) RemoveOldNotification(); };

                LayoutRoot.Children.Add(currentNotification);
                currentNotification.Display(replaceExisting);

                if (item != null && item.Level == NotificationLevel.Error)
                {
                    timer.Interval = TimeSpan.FromSeconds(6);
                }
                else
                {
                    timer.Interval = TimeSpan.FromSeconds(3);
                }

                timer.Start();
            }
        }

        private void RemoveOldNotification()
        {
            var oldView = currentNotification;

            oldView.Hide(() => LayoutRoot.Children.Remove(oldView));

            currentNotification = null;
            timer.Stop();
        }
    }
}
