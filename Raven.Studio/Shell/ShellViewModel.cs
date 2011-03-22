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

	[Export(typeof (IShell))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class ShellViewModel : Conductor<IScreen>.Collection.OneActive, IShell,
	                              IHandle<DisplayCurrentDatabaseRequested>
	{
		readonly BusyStatusViewModel busyStatus;
		readonly DatabaseViewModel databaseScreen;
		readonly IEventAggregator events;
		readonly NavigationViewModel navigation;
		readonly NotificationsViewModel notifications;

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

			Items.Add(start);
			Items.Add(databaseScreen);

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

		public BusyStatusViewModel BusyStatus { get { return busyStatus; } }

		public NotificationsViewModel Notifications { get { return notifications; } }

		public NavigationViewModel Navigation { get { return navigation; } }

		public Window Window { get { return Application.Current.MainWindow; } }

		public bool ShouldDisplaySystemButtons
		{
			get { return Application.Current.IsRunningOutOfBrowser; }
		}

		public void Handle(DisplayCurrentDatabaseRequested message)
		{
			//TODO: record the previous database so that the back button is more intuitive
			this.TrackNavigationTo(databaseScreen, events);

			navigation.Breadcrumbs.Clear();
			navigation.Breadcrumbs.Add(databaseScreen);
		}

		public void CloseWindow() { Window.Close(); }

		public void ToggleWindow()
		{
			if (!Application.Current.IsRunningOutOfBrowser) return;

			Window.WindowState = Window.WindowState == WindowState.Normal ? WindowState.Maximized : WindowState.Normal;
		}

		public void MinimizeWindow() { Window.WindowState = WindowState.Minimized; }

		public void DragWindow()
		{
			if(!Application.Current.IsRunningOutOfBrowser) return;

			Window.DragMove();
		}

		public void ResizeWindow(string direction)
		{
			if (!Application.Current.IsRunningOutOfBrowser) return;

			WindowResizeEdge edge;
			Enum.TryParse(direction, out edge);
			Window.DragResize(edge);
		}
	}
}