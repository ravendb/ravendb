namespace Raven.Studio.Collections
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Plugin;
	using Raven.Database;
	using Raven.Database.Data;
	using Raven.Client.Client;
	using Shell;

	[Export]
	public class CollectionsViewModel : Screen
	{
		readonly IServer server;
		Collection activeCollection;

		[ImportingConstructor]
		public CollectionsViewModel(IServer server)
		{
			DisplayName = "Collections";

			this.server = server;
		}

		public IEnumerable<Collection> Collections { get; private set; }
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

		void GetDocumentsForActiveCollection()
		{
			if(ActiveCollection == null)
			{
				ActiveCollectionDocuments = null;
			}
			else
			{
				ActiveCollectionDocuments = new BindablePagedQuery<DocumentViewModel>(GetDocumentsForActiveCollectionQuery);
				ActiveCollectionDocuments.GetTotalResults = () => ActiveCollection.Count;
				ActiveCollectionDocuments.LoadPage();
			}
			
			NotifyOfPropertyChange(()=>ActiveCollectionDocuments);
		}

		Task<DocumentViewModel[]> GetDocumentsForActiveCollectionQuery(int start, int pageSize)
		{
			using (var session = server.OpenSession())
			{
				var query =  new IndexQuery {Start = start, PageSize = pageSize, Query = "Tag:"+ ActiveCollection.Name};
				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync("Raven/DocumentsByEntityName", query, new string[] { })
					.ContinueWith(x =>
						{
							if(x.IsFaulted) throw new NotImplementedException("TODO");

							//TODO: this smells bad to me...
							var vm = IoC.Get<DocumentViewModel>();
							return x.Result.Results
								.Select(doc => vm.CloneUsing(doc.ToJsonDocument()))
								.ToArray();
						});
			}
		}

		protected override void OnActivate()
		{
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
					                   	});
			}
		}
	}
}