namespace Raven.Studio.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	[Export]
	public class SelectDatabaseViewModel : Screen, IRavenScreen
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

		public SectionType Section
		{
			get { return SectionType.None; }
		}

		public void OpenDatabase()
		{
			events.Publish(new ShowCurrentDatabase());
		}
	}
}