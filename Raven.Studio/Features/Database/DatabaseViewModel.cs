namespace Raven.Studio.Features.Database
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Collections;
	using Documents;
	using Framework;
	using Indexes;
	using Linq;
	using Messages;
	using Plugin;

	[Export(typeof (DatabaseViewModel))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class DatabaseViewModel : Conductor<IScreen>.Collection.OneActive,
	                                 IHandle<OpenDocumentForEdit>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public DatabaseViewModel(IServer server, IEventAggregator events)
		{
			this.server = server;
			this.events = events;
			DisplayName = "DATABASE";

			Items.Add(IoC.Get<SummaryViewModel>());
			Items.Add(IoC.Get<CollectionsViewModel>());
			Items.Add(IoC.Get<BrowseIndexesViewModel>());
			Items.Add(IoC.Get<BrowseDocumentsViewModel>());
			Items.Add(IoC.Get<LinqEditorViewModel>());

			ActivateItem(Items[0]);

			events.Subscribe(this);
		}

		public IServer Server
		{
			get { return server; }
		}

		public void Handle(OpenDocumentForEdit message)
		{
			this.TrackNavigationTo(message.Document, events);
		}

		public void Show(IScreen screen)
		{
			this.TrackNavigationTo(screen, events);
		}
	}
}