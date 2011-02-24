namespace Raven.Studio.Features.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	[Export]
	public class SelectDatabaseViewModel : Screen
	{
		readonly IEventAggregator events;

		[ImportingConstructor]
		public SelectDatabaseViewModel(IServer server, IEventAggregator events)
		{
			this.events = events;
			DisplayName = "Home";
			Server = server;
		}

		public IServer Server { get; private set; }

		public void OpenDatabase()
		{
			events.Publish(new DisplayCurrentDatabaseRequested());
		}

		public void SelectDatabase(string database)
		{
			Server.CurrentDatabase = database;
			OpenDatabase();
		}
	}
}