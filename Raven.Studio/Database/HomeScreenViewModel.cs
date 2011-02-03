namespace Raven.Studio.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Plugin;

	public class HomeScreenViewModel : Screen, IRavenScreen
	{
		public HomeScreenViewModel(IServer server)
		{
			DisplayName = "Home";
			Server = server;
			CompositionInitializer.SatisfyImports(this);
		}

		public IServer Server { get; private set; }

		public SectionType Section
		{
			get { return SectionType.None; }
		}
	}
}