namespace Raven.Studio.Shell
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Windows;
	using Caliburn.Micro;
	using Features.Database;
	using Framework;
	using Messages;
	using Plugin;

	[Export(typeof(IShell))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class ShellViewModel : Conductor<IScreen>, IShell,
		IHandle<DisplayCurrentDatabaseRequested>
	{
		readonly NavigationViewModel navigation;
		readonly NotificationsViewModel notifications;
		readonly BusyStatusViewModel busyStatus;
		readonly DatabaseViewModel databaseScreen;
		readonly IEventAggregator events;

		[ImportingConstructor]
		public ShellViewModel(
			IServer server, 
			ServerUriProvider uriProvider,
			NavigationViewModel navigation,
			NotificationsViewModel notifications,
			BusyStatusViewModel busyStatus,
			SelectDatabaseViewModel start,
			DatabaseViewModel databaseScreen,
			IEventAggregator events)
		{
			this.navigation = navigation;
			this.notifications = notifications;
			this.busyStatus = busyStatus;
			navigation.SetGoHome(() =>
									{
										this.TrackNavigationTo(start, events);
										navigation.Breadcrumbs.Clear();
									});
			this.databaseScreen = databaseScreen;
			this.events = events;
			events.Subscribe(this);
			events.Publish(new WorkStarted());

			server.Connect(new Uri(uriProvider.GetServerUri()),
				() =>
				{
					if (server.Databases.Count() == 1)
					{
						ActivateItem(databaseScreen);
					}
					else
					{
						ActivateItem(start);
					}

					events.Publish(new WorkCompleted());
				});

		}

		public BusyStatusViewModel BusyStatus {get {return busyStatus;}}

		public NotificationsViewModel Notifications
		{
			get { return notifications; }
		}

		public NavigationViewModel Navigation
		{
			get { return navigation; }
		}

		public Window Window
		{
			get { return Application.Current.MainWindow; }
		}

		public void Handle(DisplayCurrentDatabaseRequested message)
		{
			//TODO: record the previous database so that the back button is more intuitive
			this.TrackNavigationTo(databaseScreen, events);

			navigation.Breadcrumbs.Clear();
			navigation.Breadcrumbs.Add(databaseScreen);
		}

		public void CloseWindow()
		{
			Window.Close();
		}

		public void ToogleWindow()
		{
			Window.WindowState = Window.WindowState == WindowState.Normal ? WindowState.Maximized : WindowState.Normal;
		}

		public void MinimizeWindow()
		{
			Window.WindowState = WindowState.Minimized;
		}

		public void DragWindow()
		{
			Window.DragMove();
		}

		public void ResizeWindow(string direction)
		{
			WindowResizeEdge edge;
			Enum.TryParse(direction, out edge);
			Window.DragResize(edge);
		}
	}
}