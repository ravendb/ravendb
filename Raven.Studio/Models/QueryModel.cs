using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Studio.Behaviors;
using Raven.Studio.Commands;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using Raven.Studio.Extensions;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class QueryModel : PageViewModel, IHasPageTitle, IAutoCompleteSuggestionProvider
	{
        private ICommand executeQuery;
        private RavenQueryStatistics results;
        private bool skipTransformResults;
        private bool hasTransform;
        private TimeSpan queryTime;

        private IEditorDocument queryDocument;
		public QueryDocumentsCollectionSource CollectionSource { get; private set; }

		private QueryIndexAutoComplete queryIndexAutoComplete;
		protected QueryIndexAutoComplete QueryIndexAutoComplete
		{
            get { return queryIndexAutoComplete; }
			set
			{
			    queryIndexAutoComplete = value;
                queryDocument.Language.UnregisterService<ICompletionProvider>();
				queryDocument.Language.RegisterService<ICompletionProvider>(value.CompletionProvider);
			}
		}

		#region SpatialQuery

		private bool isSpatialQuerySupported;
		public bool IsSpatialQuerySupported
		{
			get { return isSpatialQuerySupported; }
			set
			{
				isSpatialQuerySupported = value;
				if (!isSpatialQuerySupported)
				{
					IsSpatialQuery = false;
				}

				OnPropertyChanged(() => IsSpatialQuerySupported);
			}
		}

		public void UpdateSpatialFields(List<SpatialQueryField> spatialFields)
		{
			IsSpatialQuerySupported = spatialFields.Count > 0;
			SpatialQuery.UpdateFields(spatialFields);
		}

		private bool isSpatialQuery;
		public bool IsSpatialQuery
		{
			get { return isSpatialQuery; }
			set
			{
				isSpatialQuery = value;
				if (!isSpatialQuery)
				{
					SpatialQuery.Clear();
				}

				OnPropertyChanged(() => IsSpatialQuery);
			}
		}

		public SpatialQueryModel SpatialQuery { get; set; }

		#endregion

		private int exceptionLine;
		public int ExceptionLine
		{
			get { return exceptionLine; }
			set { exceptionLine = value; }
		}
		private int exceptionColumn;
		public int ExceptionColumn
		{
			get { return exceptionColumn; }
			set { exceptionColumn = value; }
		}

		private string indexName;
		public string IndexName
		{
			get
			{
				return indexName;
			}
			set
			{
				if (string.IsNullOrWhiteSpace(value))
					return;

                if (HasIndexChanged(value))
                {
                    NavigateToIndexQuery(value);
                    return;
                }

			    indexName = value;
				DocumentsResult.Context = "Index/" + indexName;
				ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/indexes/" + indexName;

				SpatialQuery.IndexName = indexName;
				
				OnPropertyChanged(() => IndexName);
			}
		}

	    private static void NavigateToIndexesList()
	    {
	        UrlUtil.Navigate("/indexes");
	    }

	    private static void NavigateToIndexQuery(string indexName)
	    {
	        UrlUtil.Navigate("/query/" + indexName);
	    }

	    private bool HasIndexChanged(string newIndexName)
	    {
            if (newIndexName.StartsWith("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

	        var currentIndex = new UrlParser(UrlUtil.Url.Substring(ModelUrl.Length)).Path.Trim('/');
            return !newIndexName.Equals(currentIndex, StringComparison.OrdinalIgnoreCase);
	    }


	    private QueryOperator defaultOperator;
		public QueryOperator DefaultOperator
		{
			get { return defaultOperator; }
			set
			{
				defaultOperator = value;
				OnPropertyChanged(() => DefaultOperator);
			}
		}

		private bool showFields;

	    public bool ShowFields
	    {
	        get { return showFields; }
            set
            {
                showFields = value;
                OnPropertyChanged(() => ShowFields);
				Requery();
            }
	    }

		private bool showEntries;
		public bool ShowEntries
		{
			get { return showEntries; }
			set
			{
				showEntries = value;
				OnPropertyChanged(() => ShowEntries);
				Requery();
			}
		}

		private bool showSkipTransform = true;
		private bool useTransformer;
		public bool UseTransformer
		{
			get { return useTransformer; }
			set
			{
				useTransformer = value;
				if (value == true)
				{
					SkipTransformResults = true;
					showSkipTransform = false;
				}
				else
				{
					showSkipTransform = true;
				}
				OnPropertyChanged(() => UseTransformer);
				OnPropertyChanged(() => HasTransform);
				Requery();
			}
		}

		public Observable<string> SelectedTransformer { get; set; }
		public List<string> Transformers { get; set; } 

	    public bool SkipTransformResults
	    {
	        get { return skipTransformResults; }
            set
            {
                skipTransformResults = value;
                OnPropertyChanged(() => SkipTransformResults);
                Requery();
            }
	    }
	    #region Sorting

		public const string SortByDescSuffix = " DESC";
		public const string SortByRangeSuffix = "_Range";

		public class StringRef : NotifyPropertyChangedBase
		{
			private string value;
			public string Value
			{
				get { return value; }
				set { this.value = value; OnPropertyChanged(() => Value);}
			}
		}

		public BindableCollection<StringRef> SortBy { get; private set; }
		public BindableCollection<string> SortByOptions { get; private set; }

		public ICommand AddSortBy
		{
			get { return new ChangeFieldValueCommand<QueryModel>(this, x => x.SortBy.Add(new StringRef { Value = "" })); }
		}

		public ICommand RemoveSortBy
		{
			get { return new RemoveSortByCommand(this); }
		}

	    public ICommand AddTransformer
	    {
	        get { return (addTransformer ?? (addTransformer = new ActionCommand(() => UseTransformer = true))); }
	    }

	    public ICommand RemoveTransformer
	    {
	        get
	        {
	            return (removeTransformer ?? (removeTransformer = new ActionCommand(() =>
	            {
	                UseTransformer = false;
	                SelectedTransformer.Value = "None";
	            })));
	        }
	    }
		private class RemoveSortByCommand : Command
		{
			private string field;
			private readonly QueryModel model;

			public RemoveSortByCommand(QueryModel model)
			{
				this.model = model;
			}

			public override bool CanExecute(object parameter)
			{
				field = parameter as string;
				return field != null && model.SortBy.Any(x => x.Value == field);
			}

			public override void Execute(object parameter)
			{
				if (CanExecute(parameter) == false)
					return;

				var firstOrDefault = model.SortBy.FirstOrDefault(x => x.Value == field);
				if (firstOrDefault != null)
					model.SortBy.Remove(firstOrDefault);
			}
		}

		private void SetSortByOptions(IEnumerable<string> items)
		{
			SortByOptions.Clear();

			foreach (var item in items)
			{
				SortByOptions.Add(item);
				SortByOptions.Add(item + SortByDescSuffix); 
				SortByOptions.Add(item + SortByRangeSuffix);
				SortByOptions.Add(item + SortByRangeSuffix + SortByDescSuffix );
			}
		}
		
		#endregion

		private bool isDynamicQuery;
		public bool IsDynamicQuery
		{
			get { return isDynamicQuery; }
			set
			{
				isDynamicQuery = value;
				OnPropertyChanged(() => IsDynamicQuery);
			}
		}

		public BindableCollection<string> DynamicOptions { get; set; }

		private string dynamicSelectedOption;
	    private string queryUrl, fullQueryUrl;
	    public string DynamicSelectedOption
		{
			get { return dynamicSelectedOption; }
			set
			{
				dynamicSelectedOption = value;
				switch (dynamicSelectedOption)
				{
					case "AllDocs":
						IndexName = "dynamic";
						break;
					default:
						IndexName = "dynamic/" + dynamicSelectedOption;
						break;
				}

			    if (dynamicSelectedOption != "AllDocs")
			    {
			        BeginUpdateFieldsAndSortOptions(dynamicSelectedOption);
			    }
			    else
                {
                    SortBy.Clear();
                    SortByOptions.Clear();
                    QueryIndexAutoComplete = new QueryIndexAutoComplete(new string[0]);
                    RestoreHistory();
                }

			    OnPropertyChanged(() => DynamicSelectedOption);
			}
		}

	    private void BeginUpdateFieldsAndSortOptions(string collection)
	    {
		    DatabaseCommands.QueryAsync("Raven/DocumentsByEntityName",
		                                new IndexQuery {Query = "Tag:" + collection, Start = 0, PageSize = 1}, null)
		                    .ContinueOnSuccessInTheUIThread(result =>
		                    {
			                    if (result.Results.Count > 0)
			                    {
				                    var fields =
					                    DocumentHelpers.GetPropertiesFromJObjects(result.Results, includeNestedProperties: true,
					                                                              includeMetadata: false,
					                                                              excludeParentPropertyNames: true)
					                                   .ToList();

				                    SetSortByOptions(fields);
				                    QueryIndexAutoComplete = new QueryIndexAutoComplete(fields);
				                    RestoreHistory();
			                    }
		                    });
	    }

	    public QueryModel()
		{
			ModelUrl = "/query";
			ApplicationModel.Current.Server.Value.RawUrl = null;
            SelectedTransformer = new Observable<string> { Value = "None" };
            SelectedTransformer.PropertyChanged += (sender, args) => Requery();

		    ApplicationModel.DatabaseCommands.GetTransformersAsync(0, 256).ContinueOnSuccessInTheUIThread(transformers =>
		    {
                Transformers = new List<string>{"None"};
			    Transformers.AddRange(transformers.Select(definition => definition.Name));
			    
			    OnPropertyChanged(() => Transformers);
		    });
            
			queryDocument = new EditorDocument
            {
                Language = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef")
            };

			ExceptionLine = -1;
			ExceptionColumn = -1;
			
            CollectionSource = new QueryDocumentsCollectionSource();
		    Observable.FromEventPattern<QueryStatisticsUpdatedEventArgs>(h => CollectionSource.QueryStatisticsUpdated += h,
		                                                                 h => CollectionSource.QueryStatisticsUpdated -= h)
		        .SampleResponsive(TimeSpan.FromSeconds(0.5))
                .TakeUntil(Unloaded)
		        .ObserveOnDispatcher()
		        .Subscribe(e =>
		                       {
		                           QueryTime = e.EventArgs.QueryTime;
		                           Results = e.EventArgs.Statistics;
		                       });
		    Observable.FromEventPattern<QueryErrorEventArgs>(h => CollectionSource.QueryError += h,
		                                                     h => CollectionSource.QueryError -= h)
		        .ObserveOnDispatcher()
		        .Subscribe(e => HandleQueryError(e.EventArgs.Exception));

			DocumentsResult = new DocumentsModel(CollectionSource)
								  {
									  Header = "Results",
									  DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index, IndexName, CollectionSource.TemplateQuery),
								  };

            QueryErrorMessage = new Observable<string>();
            IsErrorVisible = new Observable<bool>();

			SortBy = new BindableCollection<StringRef>(x => x.Value);
			SortBy.CollectionChanged += HandleSortByChanged;
			SortByOptions = new BindableCollection<string>(x => x);
			Suggestions = new BindableCollection<FieldAndTerm>(x => x.Field);
			DynamicOptions = new BindableCollection<string>(x => x) {"AllDocs"};
	        AvailableIndexes = new BindableCollection<string>(x => x);
		    SpatialQuery = new SpatialQueryModel {IndexName = indexName};
		}

	    public BindableCollection<string> AvailableIndexes { get; private set; }

	    Regex errorLocation = new Regex(@"at line (\d+), column (\d+)");
	    private ICommand deleteMatchingResultsCommand;
	    private ICommand addTransformer;
	    private ICommand removeTransformer;

	    private void HandleQueryError(Exception exception)
		{
			if (exception is AggregateException)
				exception = ((AggregateException) exception).ExtractSingleInnerException();

			var indexRaven = exception.Message.IndexOf("at Raven.", System.StringComparison.Ordinal);
			var indexLucene = exception.Message.IndexOf("at Lucene.", System.StringComparison.Ordinal);
			var index = Math.Min(indexLucene, indexRaven);
			if (index != -1)
			{
				var trimmedMessage = exception.Message.Remove(index);
				QueryErrorMessage.Value = trimmedMessage;
			}
			else
			{
				QueryErrorMessage.Value = exception.Message;
			}


			var match = errorLocation.Match(QueryErrorMessage.Value);

			if (match.Success)
			{
				var success = int.TryParse(match.Groups[1].Value, out exceptionLine);
				if (success)
					success = int.TryParse(match.Groups[2].Value, out exceptionColumn);

				if (!success)
				{
					ExceptionLine = -1;
					ExceptionColumn = -1;
				}
					
			}

			IsErrorVisible.Value = true;
		}

		public void ClearQueryError()
		{
			if (Database.Value.Statistics.Value.Errors.Any(error => error.IndexName == IndexName))
			{
				QueryErrorMessage.Value = "The index " + IndexName + " has errors";
				return;
			}
			QueryErrorMessage.Value = string.Empty;
			IsErrorVisible.Value = false;
		}

	    private void HandleSortByChanged(object sender, NotifyCollectionChangedEventArgs e)
	    {
	        if (e.Action == NotifyCollectionChangedAction.Add)
	            (e.NewItems[0] as StringRef).PropertyChanged += delegate { Requery(); };
	    }

		private void Requery()
		{
			Execute.Execute(null);
		}

        public override void LoadModelParameters(string parameters)
        {
            var urlParser = new UrlParser(parameters);

            UpdateAvailableIndexes();
            ClearCurrentQuery();

            if (urlParser.GetQueryParam("mode") == "dynamic")
            {
                var collection = urlParser.GetQueryParam("collection");

                IsDynamicQuery = true;
                DatabaseCommands.GetTermsAsync("Raven/DocumentsByEntityName", "Tag", "", 100)
                    .ContinueOnSuccessInTheUIThread(collections =>
                    {
                        DynamicOptions.Match(new[] { "AllDocs" }.Concat(collections).ToArray());

                        string selectedOption = null;
                        if (!string.IsNullOrEmpty(collection))
                            selectedOption = DynamicOptions.FirstOrDefault(s => s.Equals(collection));

                        if (selectedOption == null)
                            selectedOption = DynamicOptions[0];

                        DynamicSelectedOption = selectedOption;
                        DocumentsResult.SetChangesObservable(null);
                    });

                return;
            }

            IsDynamicQuery = false;
            var newIndexName = urlParser.Path.Trim('/');

            if (string.IsNullOrEmpty(newIndexName))
            {
                if (AvailableIndexes.Any())
                {
                    NavigateToIndexQuery(AvailableIndexes.FirstOrDefault());
                    return;
                }
            }

            IndexName = newIndexName;

	        if (Database.Value.Statistics.Value.Errors.Any(error => error.IndexName == IndexName))
	        {
		        QueryErrorMessage.Value = "The index " + IndexName + " has errors";
		        IsErrorVisible.Value = true;
	        }

	        DatabaseCommands.GetIndexAsync(IndexName)
                .ContinueOnUIThread(task =>
                {
                    if (task.IsFaulted || task.Result == null)
                    {
                        if (AvailableIndexes.Any())
                        {

                            NavigateToIndexQuery(AvailableIndexes.FirstOrDefault());
                        }
                        else
                        {
                            NavigateToIndexesList();
                        }
                        return;
                    }

                    var fields = task.Result.Fields;
                    QueryIndexAutoComplete = new QueryIndexAutoComplete(fields, IndexName, QueryDocument);

	                var regex1 = new Regex(@"(?:SpatialIndex\.Generate|SpatialGenerate)");
	                var regex2 = new Regex(@"(?:SpatialIndex\.Generate|SpatialGenerate)\(@?\""([^\""]+)\""");

	                var strs = task.Result.Maps.ToList();
					if (task.Result.Reduce != null)
						strs.Add(task.Result.Reduce);
					
					var legacyFields = new HashSet<string>();
					foreach (var map in task.Result.Maps)
					{
						var count = regex1.Matches(map).Count;
						var matches = regex2.Matches(map).Cast<Match>().Select(x => x.Groups[1].Value).ToList();
						if (matches.Count < count)
							legacyFields.Add(Constants.DefaultSpatialFieldName);

						matches.ForEach(x => legacyFields.Add(x));
					}

					var spatialFields = task.Result.SpatialIndexes
						.Select(x => new SpatialQueryField
						{
							Name = x.Key,
							IsGeographical = x.Value.Type == SpatialFieldType.Geography,
							Units = x.Value.Units
						})
						.ToList();

					legacyFields.ForEach(x => spatialFields.Add(new SpatialQueryField
						{
							Name = x,
							IsGeographical = true,
							Units = SpatialUnits.Kilometers
						}));

					UpdateSpatialFields(spatialFields);

                    HasTransform = !string.IsNullOrEmpty(task.Result.TransformResults);

                    DocumentsResult.SetChangesObservable(
                        d => d.IndexChanges
                                 .Where(n =>n.Name.Equals(indexName,StringComparison.InvariantCulture))
                                 .Select(m => Unit.Default));
		
                    SetSortByOptions(fields);
                    RestoreHistory();
                }).Catch();


        }

	    private void UpdateAvailableIndexes()
	    {
            if (Database.Value == null || Database.Value.Statistics.Value == null)
            {
                return;
            }

	        AvailableIndexes.Match(Database.Value.Statistics.Value.Indexes.Select(i => i.Id.ToString()).ToArray());
	    }

	    private void ClearCurrentQuery()
	    {
	        Query = string.Empty;
            SortBy.Clear();
			SpatialQuery.Clear();
            ClearQueryError();
            Suggestions.Clear();
	        CollectionSource.Clear();
	    }

	    public bool HasTransform
	    {
            get { return hasTransform && showSkipTransform; }
            private set
            {
                hasTransform = value;
                OnPropertyChanged(() => HasTransform);
            }
	    }

	    public void RememberHistory()
	    {
            if (string.IsNullOrEmpty(IndexName))
            {
                return;
            } 

            var state = new QueryState(this);

            PerDatabaseState.QueryHistoryManager.StoreQuery(state);
		}

		public void RestoreHistory()
		{
		    var url = new UrlParser(UrlUtil.Url);
		    var recentQueryHashCode = url.GetQueryParam("recentQuery");

            if (PerDatabaseState.QueryHistoryManager.IsHistoryLoaded)
            {
                ApplyQueryState(recentQueryHashCode);
            }
            else
            {
                PerDatabaseState.QueryHistoryManager.WaitForHistoryAsync()
                    .ContinueOnUIThread(_ => ApplyQueryState(recentQueryHashCode));
            }	    
		}

        private void ApplyQueryState(string recentQueryHashCode)
	    {
            var state = string.IsNullOrEmpty(recentQueryHashCode)
                           ? PerDatabaseState.QueryHistoryManager.GetMostRecentStateForIndex(IndexName)
                           : PerDatabaseState.QueryHistoryManager.GetStateByHashCode(recentQueryHashCode);

	        if (state == null)
	            return;

	        UpdateFromState(state);

	        Requery();
	    }

		private void UpdateFromState(QueryState state)
		{
			Query = state.Query;

			IsSpatialQuery = state.IsSpatialQuery;
			SpatialQuery.UpdateFromState(state);

	        UseTransformer = state.UseTransformer;
			DefaultOperator = state.DefaultOperator;
			SelectedTransformer.Value = state.Transformer;
			SkipTransformResults = state.SkipTransform;
			ShowFields = state.ShowFields;
			ShowEntries = state.ShowEntries;


	        SortBy.Clear();

	        foreach (var sortOption in state.SortOptions)
	        {
		        if (SortByOptions.Contains(sortOption))
			        SortBy.Add(new StringRef() {Value = sortOption});
	        }
		}

		public ICommand DeleteMatchingResults { get
        {
            return deleteMatchingResultsCommand ??
                   (deleteMatchingResultsCommand = new ActionCommand(HandleDeleteMatchingResults));
        } }

	    public ICommand Execute { get { return executeQuery ?? (executeQuery = new ExecuteQueryCommand(this)); } }
		public ICommand CopyErrorTextToClipboard{get{return new ActionCommand(() => Clipboard.SetText(QueryErrorMessage.Value));}}

        public Observable<string> QueryErrorMessage { get; private set; }
        public Observable<bool> IsErrorVisible { get; private set; } 

	    public string Query
	    {
	        get { return queryDocument.CurrentSnapshot.Text; }
	        set { queryDocument.SetText(value); }
	    }

	    public TimeSpan QueryTime
		{
			get { return queryTime; }
			set
			{
				queryTime = value;
				OnPropertyChanged(() => QueryTime);
			}
		}

	    public string QueryUrl
	    {
	        get { return queryUrl; }
            set
            {
                queryUrl = value;
                OnPropertyChanged(() => QueryUrl);
            }
	    }

		public string FullQueryUrl
		{
			get { return fullQueryUrl; }
			set
			{
				fullQueryUrl = value;
				OnPropertyChanged(() => FullQueryUrl);
			}
		}

	    public RavenQueryStatistics Results
		{
			get { return results; }
			set
			{
				results = value;
				OnPropertyChanged(() => Results);
			}
		}

        public IEnumerable<FieldAndTerm> GetCurrentFieldsAndTerms()
        {
            var textSnapshotReader = queryDocument.CurrentSnapshot.GetReader(TextPosition.Zero);
            string currentField = null;
            while (!textSnapshotReader.IsAtSnapshotEnd)
            {
                var token = textSnapshotReader.ReadToken();
                if (token == null)
                    break;

                var txt = textSnapshotReader.ReadTextReverse(token.Length);
                textSnapshotReader.ReadToken();

                if (string.IsNullOrWhiteSpace(txt))
                    continue;

                string currentVal = null;
                if (token.Key == "Field")
                    currentField = txt.Substring(0, txt.Length - 1);
                else
                    currentVal = txt;

                if (currentField == null || currentVal == null)
                    continue;

                yield return new FieldAndTerm(currentField, currentVal);
                currentField = null;
            }
        }

		public DocumentsModel DocumentsResult { get; private set; }

		public BindableCollection<FieldAndTerm> Suggestions { get; private set; }
		public ICommand RepairTermInQuery
		{
			get { return new RepairTermInQueryCommand(this); }
		}

	    public IndexQuery CreateTemplateQuery()
	    {
		    var transformer = SelectedTransformer.Value;
			if (transformer == "None" || !UseTransformer)
				transformer = "";

            var q = new IndexQuery
            {
                Query = Query,
                DefaultOperator = DefaultOperator,
				ResultsTransformer = transformer
            };

            if (SortBy != null && SortBy.Count > 0)
            {
                var sortedFields = new List<SortedField>();
                foreach (var sortByRef in SortBy)
                {
                    var sortBy = sortByRef.Value;
                    if (sortBy.EndsWith(QueryModel.SortByDescSuffix))
                    {
                        var field = sortBy.Remove(sortBy.Length - QueryModel.SortByDescSuffix.Length);
                        sortedFields.Add(new SortedField(field) { Descending = true });
                    }
                    else
                        sortedFields.Add(new SortedField(sortBy));
                }
                q.SortedFields = sortedFields.ToArray();
            }

            if (ShowFields)
                q.FieldsToFetch = new[] { Constants.AllFields };

            q.DebugOptionGetIndexEntries = ShowEntries;

            q.SkipTransformResults = SkipTransformResults;



			if (IsSpatialQuerySupported && SpatialQuery.Y.HasValue && SpatialQuery.X.HasValue)
			{
				var radiusValue = SpatialQuery.Radius.HasValue ? SpatialQuery.Radius.Value : 1;

				q = new SpatialIndexQuery(q)
				{
					QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(SpatialQuery.Y.Value, SpatialQuery.X.Value, radiusValue),
					SpatialRelation = SpatialRelation.Within,
					SpatialFieldName = SpatialQuery.FieldName,
					DefaultOperator = DefaultOperator,
					RadiusUnitOverride = SpatialQuery.RadiusUnits
				};
			}

            return q;
        }

        private void HandleDeleteMatchingResults()
        {
            AskUser.ConfirmationAsync("Delete Items",
                                      "Are you sure you want to delete all documents matching the query?")
                .ContinueWhenTrueInTheUIThread(
                    () =>
                    {
                        ApplicationModel.Current.AddInfoNotification("Deleting documents");
                        DatabaseCommands.DeleteByIndexAsync(IndexName, CreateTemplateQuery(), false)
                            .ContinueOnSuccess(
                                () => ApplicationModel.Current.AddInfoNotification("Documents successfully deleted"))
                            .Catch();
                    });
        }

        protected override void OnViewLoaded()
        {
            base.OnViewLoaded();

            Database.ObservePropertyChanged()
                            .TakeUntil(Unloaded)
                            .Subscribe(_ =>
                            {
                                UpdateAvailableIndexes();

                                Database.Value.Statistics.ObservePropertyChanged()
                                        .TakeUntil(
                                            Database.ObservePropertyChanged().Select(__ => Unit.Default).Amb(Unloaded))
                                        .Subscribe(__ => UpdateAvailableIndexes());
                            });

            UpdateAvailableIndexes();
        }

		private class RepairTermInQueryCommand : Command
		{
			private readonly QueryModel model;
			private FieldAndTerm fieldAndTerm;

			public RepairTermInQueryCommand(QueryModel model)
			{
				this.model = model;
			}

			public override bool CanExecute(object parameter)
			{
				fieldAndTerm = parameter as FieldAndTerm;
				return fieldAndTerm != null;
			}

			public override void Execute(object parameter)
			{
				model.Query = model.Query.Replace(fieldAndTerm.Term, fieldAndTerm.SuggestedTerm);
				model.Requery();
			}
		}

		public string PageTitle
		{
			get { return "Query Index"; }
		}
	    public IEditorDocument QueryDocument
	    {
	        get { return queryDocument; }
	    }
		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			var list = new List<object>();
			if(PerDatabaseState.RecentAddresses.ContainsKey(IndexName))
				list = PerDatabaseState.RecentAddresses[IndexName].Keys.Cast<object>().ToList();
			return TaskEx.FromResult<IList<object>>(list);			
		}

	
	}

	public class AddressData
	{
		public string Address { get; set; }
		public double Latitude { get; set; }
		public double Longitude { get; set; }

		public override string ToString()
		{
			return Address;
		}
	}
}