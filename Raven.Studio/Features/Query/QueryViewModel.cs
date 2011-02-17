namespace Raven.Studio.Features.Query
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Client;
	using Database;
	using Documents;
	using Framework;
	using Plugin;
	using Raven.Database.Data;
	using Raven.Database.Indexing;

	[Export(typeof (IDatabaseScreenMenuItem))]
	public class QueryViewModel : Screen, IDatabaseScreenMenuItem
	{
		readonly IServer server;
		IndexDefinition currentIndex;

		[ImportingConstructor]
		public QueryViewModel(IServer server)
		{
			DisplayName = "Query";

			this.server = server;
			Indexes = new BindableCollection<IndexDefinition>();
			TermsForCurrentIndex = new BindableCollection<string>();
			QueryResults =
				new BindablePagedQuery<DocumentViewModel>(
					(start, size) => { throw new Exception("Replace this when executing the query."); });
		}

		public IObservableCollection<IndexDefinition> Indexes { get; private set; }
		public string QueryTerms { get; set; }
		public IObservableCollection<string> TermsForCurrentIndex { get; private set; }
		public BindablePagedQuery<DocumentViewModel> QueryResults { get; private set; }

		public IndexDefinition CurrentIndex
		{
			get { return currentIndex; }
			set
			{
				currentIndex = value;
				NotifyOfPropertyChange(() => CurrentIndex);
			}
		}

		public int Index
		{
			get { return 50; }
		}

		public void Execute()
		{
			QueryResults.Query = BuildQuery;
			QueryResults.LoadPage();
		}

		Task<DocumentViewModel[]> BuildQuery(int start, int pageSize)
		{
			using (var session = server.OpenSession())
			{
				var indexName = CurrentIndex.Name;
				var query = new IndexQuery
				            	{
									Start = start,
									PageSize = pageSize,
				            		Query = QueryTerms
				            	};

				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync(indexName, query, null)
					.ContinueWith(x =>
					              	{
					              		QueryResults.GetTotalResults = () => x.Result.TotalResults;

					              		return x.Result.Results
					              			.Select(obj => new DocumentViewModel(obj.ToJsonDocument()))
					              			.ToArray();
					              	});
			}
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetIndexesAsync(0, 1000)
					.ContinueOnSuccess(x => Indexes.Replace(x.Result));
			}
		}
	}
}