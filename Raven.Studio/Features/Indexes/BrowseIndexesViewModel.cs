namespace Raven.Studio.Features.Indexes
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Framework.Extensions;
	using Messages;
	using Raven.Database.Indexing;

	[Export]
	[ExportDatabaseScreen("Indexes", Index = 30)]
	public class BrowseIndexesViewModel : RavenScreen, IDatabaseScreenMenuItem,
										  IHandle<IndexUpdated>
	{
		readonly IServer server;
		IndexDefinition activeIndex;
		object activeItem;

		[ImportingConstructor]
		public BrowseIndexesViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Indexes";

			this.server = server;
			events.Subscribe(this);

			server.CurrentDatabaseChanged += delegate
			{
			    ActiveItem = null;
				if(Indexes != null) Indexes.Clear();
			};
		}

		protected override void OnInitialize()
		{
			Indexes = new BindablePagedQuery<IndexDefinition>((start, pageSize) =>
			{
				using(var session = server.OpenSession())
				return session.Advanced.AsyncDatabaseCommands
					.GetIndexesAsync(start, pageSize);
			});
		}

		protected override void OnActivate()
		{
			BeginRefreshIndexes();
		}

		public void CreateNewIndex()
		{
			ActiveItem = new EditIndexViewModel(new IndexDefinition(), server, Events);
		}

		void BeginRefreshIndexes()
		{
			WorkStarted("retrieving indexes");
			using (var session = server.OpenSession())
				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueWith(
						_ =>
							{
								WorkCompleted("retrieving indexes");
								RefreshIndexes(_.Result.CountOfIndexes);
							},
						faulted =>
							{
								WorkCompleted("retrieving indexes");
							}
						);
		}

		public BindablePagedQuery<IndexDefinition> Indexes { get; private set; }

		public IndexDefinition ActiveIndex
		{
			get { return activeIndex; }
			set
			{
				activeIndex = value;
				if (activeIndex != null)
					ActiveItem = new EditIndexViewModel(activeIndex, server, Events);
				NotifyOfPropertyChange(() => ActiveIndex);
			}
		}

		public object ActiveItem
		{
			get
			{
				return activeItem;
			}
			set
			{
				var deactivatable = activeItem as IDeactivate;
				if (deactivatable != null) deactivatable.Deactivate(close:true);

				var activatable = value as IActivate;
				if (activatable != null) activatable.Activate();

				activeItem = value; 
				NotifyOfPropertyChange(() => ActiveItem);
			}
		}

		public void Handle(IndexUpdated message)
		{
			BeginRefreshIndexes();

			if (message.IsRemoved)
			{
				ActiveItem = null;
			}
		}

		void RefreshIndexes(int totalIndexCount)
		{
			Indexes.GetTotalResults = () => totalIndexCount;
			Indexes.LoadPage();
		}
	}
}