namespace Raven.Studio.Features.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;

	[Export]
	public class SelectDatabaseViewModel : RavenScreen
	{
		[ImportingConstructor]
		public SelectDatabaseViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Home";
			Server = server;
		}

		public IServer Server { get; private set; }

		public void SelectDatabase(string database)
		{
			WorkStarted();

			Server.OpenDatabase(database, () =>
											{
												Events.Publish(new DisplayCurrentDatabaseRequested());
												WorkCompleted();
											});
		}
	}
}