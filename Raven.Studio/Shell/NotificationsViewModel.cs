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

		[ImportingConstructor]
		public NotificationsViewModel(IEventAggregator events)
		{
			events.Subscribe(this);
			notificationTimer = new DispatcherTimer {Interval = new TimeSpan(0, 0, 0, 5)};
			notificationTimer.Tick += UpdateNotifications;
			notificationTimer.Start();
			Notifications = new BindableCollection<NotificationRaised>();
		}

		public NotificationRaised MostRecent
		{
			get { return Notifications.Any() ? Notifications[0] : null; }
		}

		public BindableCollection<NotificationRaised> Notifications { get; private set; }

		void IHandle<NotificationRaised>.Handle(NotificationRaised message)
		{
			Notifications.Insert(0, message);
			NotifyOfPropertyChange( ()=> MostRecent);
		}

		void UpdateNotifications(object sender, EventArgs e)
		{
			var remove = from item in Notifications
			             where DateTime.Now - item.CreatedAt > new TimeSpan(0,0,0,5)
			             select item;
			remove.ToList().Apply(x => Notifications.Remove(x));

			NotifyOfPropertyChange(() => MostRecent);
		}
	}
}