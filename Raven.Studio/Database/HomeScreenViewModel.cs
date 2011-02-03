namespace Raven.Studio.Database
{
	using System.Collections.Generic;
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

		public IEnumerable<string> Databases
		{
			get { return Server.Databases; }
		}

		public string CurrentDatabase
		{
			get { return Server.CurrentDatabase; }
		}

		public SectionType Section
		{
			get { return SectionType.None; }
		}
	}
}