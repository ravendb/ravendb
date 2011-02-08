namespace Raven.Studio.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	public class HomeScreenViewModel : Screen, IRavenScreen
	{
		readonly IEventAggregator events;

		public HomeScreenViewModel(IServer server, IEventAggregator events)
		{
			this.events = events;
			DisplayName = "Home";
			Server = server;
			CompositionInitializer.SatisfyImports(this);
		}

		public IServer Server { get; private set; }

		public SectionType Section
		{
			get { return SectionType.None; }
		}

		public void OpenDatabase()
		{
			events.Publish(new OpenNewScreen(new SummaryViewModel(Server)));
		}
	}
}