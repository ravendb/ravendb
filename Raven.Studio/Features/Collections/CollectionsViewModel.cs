namespace Raven.Studio.Features.Collections
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Documents;
	using Framework;
	using Framework.Extensions;
	using Messages;
	using Plugins;
	using Plugins.Database;
	using Raven.Database.Data;
	using Client.Client;

	[Export(typeof(CollectionsViewModel))]
	[ExportDatabaseExplorerItem("Collections", Index = 20)]
	public class CollectionsViewModel : RavenScreen,
		IHandle<DocumentDeleted>
	{
		readonly IServer server;
		Collection activeCollection;

		[ImportingConstructor]
		public CollectionsViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Collections";

			events.Subscribe(this);

			this.server = server;

			server.CurrentDatabaseChanged += delegate
			{
				Initialize();
			};
		}

		void Initialize()
		{
			Status = "Retrieving collections";

			Collections = new BindableCollection<Collection>();

			NotifyOfPropertyChange(string.Empty);
		}

		protected override void OnInitialize()
		{
			Initialize();
		}

		public IEnumerable<Collection> Collections { get; private set; }

		BindablePagedQuery<DocumentViewModel> activeCollectionDocuments;
		public BindablePagedQuery<DocumentViewModel> ActiveCollectionDocuments
		{
			get { return activeCollectionDocuments ?? (activeCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery)); }
		}

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