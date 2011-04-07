namespace Raven.Studio.Shell
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Messages;

	[Export]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class NotificationsViewModel : PropertyChangedBase,
	                                      IHandle<NotificationRaised>
	{
		readonly DispatcherTimer notificationTimer;
		readonly TimeSpan tick = new TimeSpan(0, 0, 0, 7);

		[ImportingConstructor]
		public NotificationsViewModel(IEventAggregator events)
		{
			events.Subscribe(this);
			notificationTimer = new DispatcherTimer {Interval = tick};
			notificationTimer.Tick += UpdateNotifications;
			notificationTimer.Start();
			Notifications = new BindableCollection<NotificationRaised>();
		}

		public NotificationRaised MostRecent { get { return Notifications.Any() ? Notifications[0] : null; } }

		public bool HasErrors { get { return Notifications.Any(_ => _.Level == NotificationLevel.Error); } }

		public BindableCollection<NotificationRaised> Notifications { get; private set; }

		void IHandle<NotificationRaised>.Handle(NotificationRaised message)
		{
			Notifications.Insert(0, message);
			NotifyOfPropertyChange(() => MostRecent);
		}

		public void Dismiss(NotificationRaised message)
		{
			Notifications.Remove(message);
			NotifyOfPropertyChange(() => MostRecent);
			NotifyOfPropertyChange(() => HasErrors);
		}

		void UpdateNotifications(object sender, EventArgs e)
		{
			var remove = from item in Notifications
			             let age = DateTime.Now - item.CreatedAt
			             where age > tick && item.Level != NotificationLevel.Error
			             select item;
			remove.ToList().Apply(x => Notifications.Remove(x));

			NotifyOfPropertyChange(() => MostRecent);
			NotifyOfPropertyChange(() => HasErrors);
		}
	}
}