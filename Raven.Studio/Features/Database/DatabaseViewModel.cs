namespace Raven.Studio.Features.Database
{
	using System;
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
	public class DatabaseViewModel : Conductor<IScreen>,
		IHandle<DatabaseScreenRequested>,
		IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public DatabaseViewModel(IServer server, IEventAggregator events)
		{
			this.server = server;
			this.events = events;
			DisplayName = "DATABASE";

			//TODO: I don't want to grab these from the container
			Items = new BindableCollection<IScreen>
			        	{
			        		IoC.Get<SummaryViewModel>(),
			        		IoC.Get<CollectionsViewModel>(),
			        		IoC.Get<BrowseIndexesViewModel>(),
			        		IoC.Get<BrowseDocumentsViewModel>(),
			        		IoC.Get<LinqEditorViewModel>()
			        	};

			ActivateItem(Items[0]);

			events.Subscribe(this);
		}

		public IObservableCollection<IScreen> Items {get; private set;}

		public IServer Server
		{
			get { return server; }
		}

		public void Show(IScreen screen)
		{
			this.TrackNavigationTo(screen, events);
		}

		public void Handle(DatabaseScreenRequested message)
		{
			Show( message.GetScreen() );
		}

		public void Handle(DocumentDeleted message)
		{
			var doc = ActiveItem as EditDocumentViewModel;
			if(doc != null && doc.Id == message.DocumentId)
			{
				//TODO: this is an arbitrary choice, we should actually go back using the history
				ActiveItem = Items[3];
				doc.TryClose();

			}
		}
	}
}