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
	using Framework;
	using Plugin;
	using Raven.Database.Data;

	[Export]
	public class CollectionsViewModel : RavenScreen
	{
		readonly IServer server;
		Collection activeCollection;

		[ImportingConstructor]
		public CollectionsViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Collections";

			this.server = server;
			SystemCollections = new BindableCollection<Collection>();
		}

		public IEnumerable<Collection> Collections { get; private set; }
		public BindableCollection<Collection> SystemCollections { get; private set; }
		public BindablePagedQuery<DocumentViewModel> ActiveCollectionDocuments { get; private set; }

		public Collection ActiveCollection
		{
			get { return activeCollection; }
			set
			{
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
			if (ActiveCollection == null)
			{
				ActiveCollectionDocuments = null;
			}
			else
			{
				ActiveCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery);
				ActiveCollectionDocuments.GetTotalResults = () => ActiveCollection.Count;
				ActiveCollectionDocuments.LoadPage();
			}

			NotifyOfPropertyChange(() => ActiveCollectionDocuments);
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

										//TODO: this smells bad to me...
										var vm = IoC.Get<DocumentViewModel>();
										return x.Result.Results
											.Select(doc => vm.CloneUsing(doc.ToJsonDocument()))
											.ToArray();
									});
			}
		}

		readonly Collection raven = new Collection { Name = "Raven", Count = 0 };

		public Collection OrphansCollection
		{
			get;
			set;
		}

		public Collection RavenCollection
		{
			get { return raven; }
		}

		protected override void OnActivate()
		{
			WorkStarted();

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueOnSuccess(x =>
										{
											Collections = x.Result;
											NotifyOfPropertyChange(() => LargestCollectionCount);
											NotifyOfPropertyChange(() => Collections);

											ActiveCollection = Collections.FirstOrDefault();
											NotifyOfPropertyChange(() => HasCollections);

											WorkCompleted();
										});

				session.Advanced.AsyncDatabaseCommands
					.QueryAsync("Raven/OrphanDocuments", new IndexQuery { PageSize = 0, Start = 0 }, null)
					.ContinueOnSuccess(x =>
										{
											var c = new Collection{Count = x.Result.TotalResults,Name = "Orphans"};
											OrphansCollection = c;
											NotifyOfPropertyChange(() => OrphansCollection);
										});
			}
		}
	}
}