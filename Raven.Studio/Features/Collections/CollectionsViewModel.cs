namespace Raven.Studio.Features.Collections
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Client.Client;
	using Database;
	using Documents;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database.Data;

	[Export(typeof(IDatabaseScreenMenuItem))]
	[Export(typeof(CollectionsViewModel))]
	public class CollectionsViewModel : RavenScreen, IDatabaseScreenMenuItem,
		IHandle<DocumentDeleted>
	{
		readonly Collection raven = new Collection { Name = "Raven", Count = 0 };
		readonly IServer server;
		Collection activeCollection;

		public int Index { get { return 20; } }

		[ImportingConstructor]
		public CollectionsViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Collections";

			events.Subscribe(this);

			this.server = server;
			SystemCollections = new BindableCollection<Collection>();
			ActiveCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery);
		}

		public IEnumerable<Collection> Collections { get; private set; }
		public BindableCollection<Collection> SystemCollections { get; private set; }
		public BindablePagedQuery<DocumentViewModel> ActiveCollectionDocuments { get; private set; }

		public Collection ActiveCollection
		{
			get { return activeCollection; }
			set
			{
				if (activeCollection == value) return;

				activeCollection = value;
				NotifyOfPropertyChange(() => ActiveCollection);
				GetDocumentsForActiveCollection();
			}
		}

		public long LargestCollectionCount
		{
			get
			{
				return (Collections == null || !Collections.Any())
						? 0
						: Collections.Max(x => x.Count);
			}
		}

		public bool HasCollections
		{
			get { return Collections != null && Collections.Any(); }
		}

		public Collection RavenCollection
		{
			get { return raven; }
		}

		void GetDocumentsForActiveCollection()
		{
			if (ActiveCollection == null) return;

			ActiveCollectionDocuments.GetTotalResults = () => ActiveCollection.Count;
			ActiveCollectionDocuments.LoadPage();
		}

		Task<DocumentViewModel[]> GetDocumentsForActiveCollectionQuery(int start, int pageSize)
		{
			WorkStarted();

			using (var session = server.OpenSession())
			{
				var query = new IndexQuery { Start = start, PageSize = pageSize, Query = "Tag:" + ActiveCollection.Name };
				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync("Raven/DocumentsByEntityName", query, new string[] { })
					.ContinueWith(x =>
									{
										if (x.IsFaulted) throw new NotImplementedException("TODO");

										WorkCompleted();

										return x.Result.Results
											.Select(obj => new DocumentViewModel(obj.ToJsonDocument()))
											.ToArray();
									});
			}
		}

		protected override void OnActivate()
		{
			WorkStarted();

			var currentActiveCollection = ActiveCollection;
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueOnSuccess(x =>
										{
											Collections = x.Result;
											NotifyOfPropertyChange(() => LargestCollectionCount);
											NotifyOfPropertyChange(() => Collections);

											ActiveCollection = currentActiveCollection ?? Collections.FirstOrDefault();
											NotifyOfPropertyChange(() => HasCollections);

											WorkCompleted();
										});

				//session.Advanced.AsyncDatabaseCommands
				//    .GetDocumentsStartingWithAsync("Raven",0,1)
				//    .ContinueOnSuccess(x =>
				//                        {
				//                            var c = new Collection { Count = x.Result.TotalResults, Name = "Orphans" };
				//                            OrphansCollection = c;
				//                            NotifyOfPropertyChange(() => OrphansCollection);
				//                        });
			}
		}

		public void EditTemplate()
		{
			var vm = IoC.Get<EditCollectionTemplateViewModel>();
			vm.Collection = ActiveCollection;
			Events.Publish(new DatabaseScreenRequested(() => vm));
		}

		public void Handle(DocumentDeleted message)
		{
			ActiveCollectionDocuments
				.Where(x => x.Id == message.DocumentId)
				.ToList()
				.Apply(x => ActiveCollectionDocuments.Remove(x));
		}
	}
}