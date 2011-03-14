namespace Raven.Studio.Features.Query
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Database;
	using Documents;
	using Framework;
	using Plugin;
	using Raven.Database.Data;

	public class QueryViewModel : Screen, IDatabaseScreenMenuItem
	{
		readonly List<string> dynamicIndex = new List<string>();
		readonly IServer server;
		string currentIndex;
		string queryResultsStatus;
		bool shouldShowDynamicIndexes;

		[ImportingConstructor]
		public QueryViewModel(IServer server)
		{
			DisplayName = "Query";

			this.server = server;
			Indexes = new BindableCollection<string>();
			TermsForCurrentIndex = new BindableCollection<string>();
			QueryResults =
				new BindablePagedQuery<DocumentViewModel>(
					(start, size) => { throw new Exception("Replace this when executing the query."); });
		}

		public IObservableCollection<string> Indexes { get; private set; }
		public string QueryTerms { get; set; }
		public IObservableCollection<string> TermsForCurrentIndex { get; private set; }
		public BindablePagedQuery<DocumentViewModel> QueryResults { get; private set; }

		public string QueryResultsStatus
		{
			get { return queryResultsStatus; }
			set
			{
				queryResultsStatus = value;
				NotifyOfPropertyChange(() => QueryResultsStatus);
			}
		}

		public string CurrentIndex
		{
			get { return currentIndex; }
			set
			{
				currentIndex = value;
				NotifyOfPropertyChange(() => CurrentIndex);
				NotifyOfPropertyChange(() => CanExecute);
			}
		}

		public bool ShouldShowDynamicIndexes
		{
			get { return shouldShowDynamicIndexes; }
			set
			{
				shouldShowDynamicIndexes = value;
				NotifyOfPropertyChange(() => ShouldShowDynamicIndexes);
				GetIndexNames();
			}
		}

		public bool CanExecute
		{
			get { return !string.IsNullOrEmpty(CurrentIndex); }
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
				var indexName = CurrentIndex;
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

					              		QueryResultsStatus = DetermineResultsStatus(x.Result);


					              		//maybe we added a temp index?
					              		if (indexName.StartsWith("dynamic"))
					              			GetIndexNames();

					              		return x.Result.Results
					              			.Select(obj => new DocumentViewModel(obj.ToJsonDocument()))
					              			.ToArray();
					              	});
			}
		}

		static string DetermineResultsStatus(QueryResult result)
		{
			//TODO: give the user some info about skipped results, etc?
			if (result.TotalResults == 0) return "No documents matched the query.";
			if (result.TotalResults == 1) return "1 document found.";
			return string.Format("{0} documents found.", result.TotalResults);
		}

		protected override void OnActivate()
		{
			GetIndexNames();
		}

		void ReplaceVisibleList(IEnumerable<string> newList)
		{
			string oldSelection = CurrentIndex;
			Indexes.Replace(newList);
			CurrentIndex = oldSelection;
		}

		void GetIndexNames()
		{
			if (ShouldShowDynamicIndexes)
			{
				ShowDynamicIndexes();
			}
			else
			{
				using (var session = server.OpenSession())
				{
					session.Advanced.AsyncDatabaseCommands
						.GetIndexNamesAsync(0, 1000)
						.ContinueWith(x => ReplaceVisibleList(x.Result));
				}
			}
		}

		void ShowDynamicIndexes()
		{
			if (dynamicIndex.Count != 0)
			{
				ReplaceVisibleList(dynamicIndex);
				return;
			}

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 250)
					.ContinueWith(task =>
					{
					    foreach (var collection in task.Result)
					    {
					        dynamicIndex.Insert(0, "dynamic/" + collection.Name);
					    }

					    dynamicIndex.Insert(0, "dynamic");

					    ReplaceVisibleList(dynamicIndex);
					});
			}
		}
	}
}