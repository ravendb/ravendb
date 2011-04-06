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
	using Framework.Extensions;
	using Raven.Database.Data;
	using Client.Extensions;
	using Client.Client;


	[ExportDatabaseScreen("Query", Index = 50)]
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

		    FieldsForCurrentIndex = new BindableCollection<string>();
		    FieldsForCurrentIndex.CollectionChanged += delegate { NotifyOfPropertyChange(() => HasFields); };

		    TermsForCurrentField = new BindableCollection<string>();
		    TermsForCurrentField.CollectionChanged += delegate { NotifyOfPropertyChange(() => HasSuggestedTerms); };

			QueryResults =
				new BindablePagedQuery<DocumentViewModel>(
					(start, size) => { throw new Exception("Replace this when executing the query."); });

			shouldShowDynamicIndexes = true;
		}

		public IObservableCollection<string> Indexes { get; private set; }
	    private string query;
	    public string Query
	    {
	        get { return query; }
	        set { query = value; NotifyOfPropertyChange(()=>Query); }
	    }

	    public IObservableCollection<string> TermsForCurrentField { get; private set; }
		public IObservableCollection<string> FieldsForCurrentIndex { get; private set; }
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

	    public bool HasCurrentIndex
	    {
            get { return !string.IsNullOrEmpty(CurrentIndex); }
	    }

		public string CurrentIndex
		{
			get { return currentIndex; }
			set
			{
				currentIndex = value;
				NotifyOfPropertyChange(() => CurrentIndex);
				NotifyOfPropertyChange(() => CanExecute);
                NotifyOfPropertyChange(() => HasCurrentIndex);

                QueryResults.ClearResults();
			    QueryResultsStatus = string.Empty;

				if(!string.IsNullOrEmpty(currentIndex)) GetFieldsForCurrentIndex();
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
				var q = new IndexQuery
				            	{
				            		Start = start,
				            		PageSize = pageSize,
				            		Query = Query
				            	};

				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync(indexName, q, null)
					.ContinueWith(x =>
					              	{
										if (x.Exception != null)
										{
											QueryResultsStatus = x.Exception.ExtractSingleInnerException().SimplifyError();
											return new DocumentViewModel[]{};
										}
						
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

		void GetFieldsForCurrentIndex()
		{
            FieldsForCurrentIndex.Clear();

			using (var session = server.OpenSession())
			session.Advanced.AsyncDatabaseCommands
					.GetIndexAsync(CurrentIndex)
					.ContinueWith(x => FieldsForCurrentIndex.AddRange(x.Result.Fields));
		}

        void GetTermsForCurrentField()
        {
            TermsForCurrentField.Clear();
            using (var session = server.OpenSession())
                session.Advanced.AsyncDatabaseCommands
                        .GetTermsAsync(CurrentIndex, CurrentField, fromValue:string.Empty, pageSize:20)
                        .ContinueWith(x => TermsForCurrentField.AddRange(x.Result));
        }

        public void AddFieldToQuery(string field)
        {
            if (!string.IsNullOrEmpty(Query)) field = " " + field;
            field += ":";
            Query += field;
        }

        public void AddTermToQuery(string term)
        {
            var q = (Query ?? string.Empty).Trim();
            var field = CurrentField + ":";
            if (!q.EndsWith(field))
                Query += field + " \"" + term + "\"";
            else
                Query += term;
        }

	    private string currentField;
	    public string CurrentField
	    {
	        get { return currentField; }
	        set
	        {
	            currentField = value; 
                NotifyOfPropertyChange(()=>CurrentField);
                if (!string.IsNullOrEmpty(currentField)) GetTermsForCurrentField();
	        }
	    }

	    public bool HasFields
	    {
            get { return FieldsForCurrentIndex.Any(); }
	    }

	    public bool HasSuggestedTerms
	    {
            get { return TermsForCurrentField.Any(); }
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
            if( !newList.Except(Indexes).Any() ) return;

            string oldSelection = currentIndex;
			Indexes.Replace(newList);
            currentIndex = oldSelection;
		}

		void GetIndexNames()
		{
			if(!server.IsInitialized) return;

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