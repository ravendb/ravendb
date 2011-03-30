namespace Raven.Studio.Features.Collections
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Client.Extensions;
	using Database;
	using Documents;
	using Framework;
	using Messages;
	using Raven.Database.Data;

	[Export(typeof(CollectionsViewModel))]
	public class CollectionsViewModel : RavenScreen, IDatabaseScreenMenuItem,
		IHandle<DocumentDeleted>
	{
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

			server.CurrentDatabaseChanged += delegate
			{
				Collections = new BindableCollection<Collection>();
				ActiveCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery);

				NotifyOfPropertyChange(string.Empty);
			};	
		}

		public IEnumerable<Collection> Collections { get; private set; }
		public BindablePagedQuery<DocumentViewModel> ActiveCollectionDocuments { get; private set; }

		string status;
		public string Status
		{
			get { return status; }
			set { status = value; NotifyOfPropertyChange(() => Status); }
		}

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

		void GetDocumentsForActiveCollection()
		{
			if (ActiveCollection == null) return;

			ActiveCollectionDocuments.GetTotalResults = () => ActiveCollection.Count;
			ActiveCollectionDocuments.LoadPage();
		}

		Task<DocumentViewModel[]> GetDocumentsForActiveCollectionQuery(int start, int pageSize)
		{
			WorkStarted("retrieving documents for collection.");

			using (var session = server.OpenSession())
			{
				var query = new IndexQuery { Start = start, PageSize = pageSize, Query = "Tag:" + ActiveCollection.Name };
				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync("Raven/DocumentsByEntityName", query, new string[] { })
					.ContinueWith(x =>
									{
										WorkCompleted("retrieving documents for collection.");

										if (x.IsFaulted) throw new NotImplementedException("TODO");

										return x.Result.Results
											.Select(obj => new DocumentViewModel(obj.ToJsonDocument()))
											.ToArray();
									});
			}
		}

		protected override void OnActivate()
		{
			WorkStarted("fetching collections");
			Status = "Retrieving collections";

			var currentActiveCollection = ActiveCollection;
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueWith(
					x =>
					{
						WorkCompleted("fetching collections");

						Collections = x.Result;
						NotifyOfPropertyChange(() => LargestCollectionCount);
						NotifyOfPropertyChange(() => Collections);

						ActiveCollection = currentActiveCollection ?? Collections.FirstOrDefault();
						NotifyOfPropertyChange(() => HasCollections);

						Status = Collections.Any() ? string.Empty : "The database contains no collections.";
					},
					faulted =>
					{
						WorkCompleted("fetching collections");
						var error = "Unable to retrieve collections from server.";
						Status = error;
						NotifyError(error);
					});
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