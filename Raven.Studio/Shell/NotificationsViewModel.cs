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
		readonly TimeSpan tick = new TimeSpan(0, 0, 0, 2);
		readonly TimeSpan dismissAfter = new TimeSpan(0, 0, 0, 7);

		[ImportingConstructor]
		public NotificationsViewModel(IEventAggregator events)
		{
			events.Subscribe(this);
			notificationTimer = new DispatcherTimer { Interval = tick };
			notificationTimer.Tick += HandleTick;
			notificationTimer.Start();
			Notifications = new BindableCollection<NotificationRaised>();
		}

		NotificationRaised mostRecent;
		public NotificationRaised MostRecent
		{
			get { return mostRecent; }
			private set
			{
				mostRecent = value;
				NotifyOfPropertyChange(() => MostRecent);
			}
		}

		public bool HasErrors { get { return Notifications.Any(_ => _.Level == NotificationLevel.Error); } }

		public BindableCollection<NotificationRaised> Notifications { get; private set; }

		void IHandle<NotificationRaised>.Handle(NotificationRaised message)
		{
			Notifications.Insert(0, message);
			MostRecent = message;
		}

		public void Dismiss(NotificationRaised message)
		{
			Notifications.Remove(message);

			if (message == MostRecent) MostRecent = null;

			NotifyOfPropertyChange(() => HasErrors);
		}

		void HandleTick(object sender, EventArgs e)
		{
			if (MostRecent == null) return;
			if (DateTime.Now - MostRecent.CreatedAt > dismissAfter) MostRecent = null;
		}
	}
}