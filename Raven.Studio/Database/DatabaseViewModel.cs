namespace Raven.Studio.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Documents;
	using Indexes;
	using Plugin;

	[Export(typeof (DatabaseViewModel))]
	public class DatabaseViewModel : Conductor<IScreen>.Collection.OneActive
	{
		readonly IServer server;

		[ImportingConstructor]
		public DatabaseViewModel(IServer server)
		{
			this.server = server;
			DisplayName = "DATABASE";

			Items.Add(IoC.Get<SummaryViewModel>());
			Items.Add(IoC.Get<BrowseIndexesViewModel>());
			Items.Add(IoC.Get<BrowseDocumentsViewModel>());

			ActivateItem(Items[0]);
		}

		public void Show(IScreen screen)
		{
			ActivateItem(screen);
		}

		public IServer Server
		{
			get { return server; }
		}
	}
}