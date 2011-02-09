namespace Raven.Studio.Database
{
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;

	public class HomeScreenViewModel : Screen, IRavenScreen
	{
		readonly IEventAggregator events;
		readonly TemplateColorProvider colorProvider;

		public HomeScreenViewModel(IServer server, IEventAggregator events, TemplateColorProvider colorProvider)
		{
			this.events = events;
			this.colorProvider = colorProvider;
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
			events.Publish(new OpenNewScreen(new SummaryViewModel(Server, colorProvider)));
		}
	}
}