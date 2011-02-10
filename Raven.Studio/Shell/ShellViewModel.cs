namespace Raven.Studio.Shell
{
	using System;
	using System.ComponentModel.Composition;
	using System.Windows;
	using Caliburn.Micro;
	using Database;
	using Messages;
	using Plugin;

	[Export(typeof (IShell))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class ShellViewModel : Conductor<IScreen>.Collection.OneActive, IShell, IHandle<ShowCurrentDatabase>
	{
		readonly SummaryViewModel summary;

		[ImportingConstructor]
		public ShellViewModel(IServer server, SelectDatabaseViewModel start, SummaryViewModel summary, IEventAggregator events)
		{
			this.summary = summary;
			server.Connect(new Uri("http://localhost:8080"));
			ActivateItem(start);
			events.Subscribe(this);

			Items.Add(summary);
		}

		public Window Window
		{
			get { return Application.Current.MainWindow; }
		}

		public void Handle(ShowCurrentDatabase message)
		{
			ActivateItem(summary);
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