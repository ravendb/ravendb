namespace Raven.ManagementStudio.UI.Silverlight.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Plugin;

	public class HomeScreenViewModel : Screen, IRavenScreen
	{
		public HomeScreenViewModel(IDatabase database)
		{
			DisplayName = "Home";
			Database = database;
			CompositionInitializer.SatisfyImports(this);
		}

		public IDatabase Database { get; set; }

		public IRavenScreen ParentRavenScreen { get; set; }

		public SectionType Section
		{
			get { return SectionType.None; }
		}
	}
}