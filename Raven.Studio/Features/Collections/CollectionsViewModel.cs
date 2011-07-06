using Raven.Client.Connection;
using Raven.Studio.Infrastructure.Navigation;

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

	[Export]
	[ExportDatabaseExplorerItem(DisplayName = "Collections", Index = 20)]
	public class CollectionsViewModel : RavenScreen,
		IHandle<DocumentDeleted>
	{
		CollectionViewModel activeCollection;

		[ImportingConstructor]
		public CollectionsViewModel()
		{
			DisplayName = "Collections";

			Events.Subscribe(this);

			Server.CurrentDatabaseChanged += delegate
			{
				Initialize();
			};
		}

		void Initialize()
		{
			Status = "Retrieving collections";

			Collections = new BindableCollection<CollectionViewModel>();

			NotifyOfPropertyChange(string.Empty);
		}

		protected override void OnInitialize()
		{
			Initialize();
		}

		public IObservableCollection<CollectionViewModel> Collections { get; private set; }

		BindablePagedQuery<DocumentViewModel> activeCollectionDocuments;
		public BindablePagedQuery<DocumentViewModel> ActiveCollectionDocuments
		{
			get { return activeCollectionDocuments ?? (activeCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery)); }
		}

		string status;
		private System.Action executeAfterCollectionsFetched;

		public string Status
		{
			get { return status; }
			set { status = value; NotifyOfPropertyChange(() => Status); }
		}

		public CollectionViewModel ActiveCollection
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
			TrackCurrentCollection();

			ActiveCollectionDocuments.ClearResults();

			if (ActiveCollection == null) return;

			ActiveCollectionDocuments.GetTotalResults = () => ActiveCollection.Count;
			ActiveCollectionDocuments.LoadPage();
		}

		private void TrackCurrentCollection()
		{
			if (ActiveCollection == null)
				return;

			Execute.OnUIThread(() =>
							   NavigationService.Track(new NavigationState
														{
															Url = string.Format("collections/{0}", ActiveCollection.Name),
															Title = string.Format("Collections: {0}", ActiveCollection.Name)
														}));
		}

		Task<DocumentViewModel[]> GetDocumentsForActiveCollectionQuery(int start, int pageSize)
		{
			WorkStarted("retrieving documents for collection.");

			using (var session = Server.OpenSession())
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

		public void SelectCollectionByName(string name)
		{
			if (HasCollections)
			{
				var collection = Collections
					.Where(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase))
					.FirstOrDefault();

				if (collection == null)
					return;

				ActiveCollection = collection;
				return;
			}
			executeAfterCollectionsFetched = () => SelectCollectionByName(name);
		}

		protected override void OnActivate()
		{
			WorkStarted("fetching collections");

			using (var session = Server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueWith(
					x =>
					{
						WorkCompleted("fetching collections");

						Collections = new BindableCollection<CollectionViewModel>(
							x.Result.Select(item => new CollectionViewModel { Name = item.Name, Count = item.Count }));

						NotifyOfPropertyChange(() => LargestCollectionCount);
						NotifyOfPropertyChange(() => Collections);
						NotifyOfPropertyChange(() => HasCollections);

						if (ActiveCollection != null)
						{
							activeCollection = Collections
								.Where(collection => collection.Name == ActiveCollection.Name)
								.FirstOrDefault();
							NotifyOfPropertyChange(() => ActiveCollection);
							TrackCurrentCollection();
						}
						else // select the first one if we weren't asked for one
						{
							ActiveCollection = Collections.FirstOrDefault();
						}
						if (executeAfterCollectionsFetched != null)
						{
							executeAfterCollectionsFetched();
							executeAfterCollectionsFetched = null;
						}

						Status = Collections.Any() ? string.Empty : "The database contains no collections.";
					},
					faulted =>
					{
						WorkCompleted("fetching collections");
						const string error = "Unable to retrieve collections from server."; ;
						Status = error;
						NotifyError(error);
					});
			}
		}

		public void EditTemplate()
		{
			var vm = IoC.Get<EditCollectionTemplateViewModel>();
			vm.Collection = new Collection { Name = ActiveCollection.Name, Count = ActiveCollection.Count };
			Events.Publish(new DatabaseScreenRequested(() => vm));
		}

		void IHandle<DocumentDeleted>.Handle(DocumentDeleted message)
		{
			ActiveCollectionDocuments
				.Where(x => x.Id == message.DocumentId)
				.ToList()
				.Apply(x => ActiveCollectionDocuments.Remove(x));

			ActiveCollection.Count -= 1;
			if (ActiveCollection.Count == 0)
			{
				Collections.Remove(ActiveCollection);
			}
		}
	}
}