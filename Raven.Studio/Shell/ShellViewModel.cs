namespace Raven.Studio.Shell
{
	using System;
	using System.ComponentModel.Composition;
	using System.Windows;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Plugin;

	[Export(typeof (IShell))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class ShellViewModel : Conductor<IScreen>.Collection.OneActive, IShell, 
		IHandle<ShowCurrentDatabase>
	{
		readonly NavigationViewModel navigation;
		readonly SummaryViewModel summary;
		readonly IEventAggregator events;

		[ImportingConstructor]
		public ShellViewModel(IServer server, NavigationViewModel navigation, SelectDatabaseViewModel start, SummaryViewModel summary, IEventAggregator events)
		{
			this.navigation = navigation;
			navigation.SetGoHome( ()=>
			                      	{
										this.TrackNavigationTo(start, events);
			                      		navigation.Breadcrumbs.Clear();
			                      	});
			this.summary = summary;
			this.events = events;

			server.Connect(new Uri("http://localhost:8080"));
			ActivateItem(start);
			events.Subscribe(this);

			Items.Add(summary);
		}

		public NavigationViewModel Navigation
		{
			get { return navigation; }
		}

		public Window Window
		{
			get { return Application.Current.MainWindow; }
		}

		public void Handle(ShowCurrentDatabase message)
		{
			//TODO: record the previous database so that the back button is more intuitive
			this.TrackNavigationTo(summary, events);

			navigation.Breadcrumbs.Clear();
			navigation.Breadcrumbs.Add(summary);
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