using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Controls;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;
using Notification = Raven.Studio.Messages.Notification;

namespace Raven.Studio.Models
{
	public class ReportingModel : PageViewModel, IHasPageTitle
	{
        private static readonly FacetAggregation[] AllSummaryModes = new[]
	                            {
	                                FacetAggregation.Count, FacetAggregation.Min, FacetAggregation.Max,
	                                FacetAggregation.Sum,
	                                FacetAggregation.Average
	                            };

		public string PageTitle { get; private set; }

        static ReportingModel()
        {
            QueryLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
        }

		public ReportingModel()
		{
			PageTitle = "Reporting";
			ModelUrl = "/reporting";
			AvailableIndexes = new BindableCollection<string>(x => x);
			IndexFields = new BindableCollection<string>(x => x);
            Results = new ObservableCollection<ReportRow>();
            GroupByField = new Observable<string>();
            ValueCalculations = new ObservableCollection<ValueCalculation>();
		    ExecutionElapsedTime = new Observable<TimeSpan>();
            IsFilterVisible = new Observable<bool>();
            QueryErrorMessage = new Observable<string>();
            IsErrorVisible = new Observable<bool>();

            FilterDoc = new EditorDocument
            {
                Language = QueryLanguage
            };
		}

	    public Observable<TimeSpan> ExecutionElapsedTime { get; private set; }
        public Observable<string> QueryErrorMessage { get; private set; }
        public Observable<bool> IsErrorVisible { get; private set; } 

	    public EditorDocument FilterDoc { get; private set; }
        private QueryIndexAutoComplete queryIndexAutoComplete;
	    private string indexName;
	    private ColumnsModel resultColumns;
	    private ICommand deleteValueCalculation;
	    private static ISyntaxLanguage QueryLanguage;
	    private ICommand addFilter;
	    private ICommand deleteFilter;
	    private ICommand exportReport;

	    protected QueryIndexAutoComplete QueryIndexAutoComplete
        {
            get { return queryIndexAutoComplete; }
            set
            {
                queryIndexAutoComplete = value;
                FilterDoc.Language.UnregisterService<ICompletionProvider>();
                FilterDoc.Language.RegisterService(value.CompletionProvider);
            }
        }

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
					NavigateToIndexReport(value);
					return;
				}

				indexName = value;
				ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/indexes/" + indexName;

				OnPropertyChanged(() => IndexName);
			}
		}

        public Observable<bool> IsFilterVisible { get; private set; }
 
		private static void NavigateToIndexesList()
		{
			UrlUtil.Navigate("/indexes");
		}

		private static void NavigateToIndexReport(string indexName)
		{
			UrlUtil.Navigate("/reporting/" + indexName);
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

		public BindableCollection<string> AvailableIndexes { get; private set; }

		private void UpdateAvailableIndexes()
		{
			if (Database.Value == null || Database.Value.Statistics.Value == null)
			{
				return;
			}

			AvailableIndexes.Match(Database.Value.Statistics.Value.Indexes.Select(i => i.Id.ToString()).ToArray());
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);

		    ExecutionElapsedTime.Value = TimeSpan.Zero;
            GroupByField.Value = "";
            ValueCalculations.Clear();

			UpdateAvailableIndexes();

			var newIndexName = urlParser.Path.Trim('/');

			if (string.IsNullOrEmpty(newIndexName))
			{
				if (AvailableIndexes.Any())
				{
					NavigateToIndexReport(AvailableIndexes.FirstOrDefault());
					return;
				}
			}

			IndexName = newIndexName;

			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnUIThread(task =>
				{
                    if (task.IsFaulted || task.Result == null)
                    {
                        if (AvailableIndexes.Any())
                        {
                            NavigateToIndexReport(AvailableIndexes.FirstOrDefault());
                        }
                        else
                        {
                            NavigateToIndexesList();
                        }

                        return;
                    }

				    var fields = task.Result.Fields;

				    IndexFields.Match(fields);
				    QueryIndexAutoComplete = new QueryIndexAutoComplete(fields, IndexName, FilterDoc);
				}).Catch();
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

		public ICommand ExecuteReportCommand{get{return new AsyncActionCommand(ExecuteReport);}}

	    private async Task ExecuteReport()
	    {
            QueryErrorMessage.Value = "";
            IsErrorVisible.Value = false;

	        if (string.IsNullOrEmpty(GroupByField.Value))
	        {
	            ApplicationModel.Current.Notifications.Add(new Notification("You must select a field to group by"));
	            return;
	        }

            if (ValueCalculations.Count == 0)
            {
                ApplicationModel.Current.Notifications.Add(new Notification("You must add at least one Value"));
                return;
            }

	        var facets = new List<AggregationQuery>();

            foreach (var value in ValueCalculations)
            {
                var facetForField = facets.FirstOrDefault(f => f.AggregationField == value.Field);
                if (facetForField != null)
                {
                    facetForField.Aggregation |= value.SummaryMode;
                }
                else
                {
                    facets.Add(new AggregationQuery
                    {
                        Name = GroupByField.Value,
                        DisplayName = GroupByField.Value + "-" + value.Field,
                        AggregationField = value.Field,
                        Aggregation = value.SummaryMode
                    });
                }
            }

            ResultColumns = null;
	        Results.Clear();

	        var cancelationTokenSource = new CancellationTokenSource();

	        var progressWindow = new ProgressWindow()
	        {
	            Title = "Preparing Report",
	            IsIndeterminate = false,
	            CanCancel = true
	        };

	        progressWindow.Closed += delegate { cancelationTokenSource.Cancel(); };
	        progressWindow.Show();

	        var queryStartTime = DateTime.UtcNow.Ticks;

	        try
	        {
	            var results = new List<KeyValuePair<string, FacetResult>>();
	            var hasMoreResults = true;
	            var fetchedResults = 0;

	            while (hasMoreResults)
	            {
	                var queryFacetsTask = DatabaseCommands.GetFacetsAsync(IndexName,
	                                                                      new IndexQuery() {Query = FilterDoc.Text},
	                                                                      AggregationQuery.GetFacets(facets),
	                                                                      fetchedResults, 256);

	                await TaskEx.WhenAny(
	                    queryFacetsTask,
	                    TaskEx.Delay(int.MaxValue, cancelationTokenSource.Token));

	                if (cancelationTokenSource.IsCancellationRequested)
	                {
	                    return;
	                }

	                var facetResults = await queryFacetsTask;


	                results.AddRange(facetResults.Results);

	                fetchedResults += facetResults.Results.Select(r => r.Value.Values.Count).Max();
	                var remainingResults = facetResults.Results.Select(r => r.Value.RemainingTermsCount).Max();
	                var totalResults = fetchedResults + remainingResults;

	                progressWindow.Progress = (int) ((fetchedResults/(double) totalResults)*100);

	                hasMoreResults = remainingResults > 0;
	            }

	            var rowsByKey = new Dictionary<string, ReportRow>();
	            var rows = new List<ReportRow>();

	            foreach (var facetResult in results)
	            {
	                var calculatedField = facetResult.Key.Split('-')[1];

	                foreach (var facetValue in facetResult.Value.Values)
	                {
	                    ReportRow result;
	                    if (!rowsByKey.TryGetValue(facetValue.Range, out result))
	                    {
	                        result = new ReportRow {Key = facetValue.Range};
	                        rowsByKey.Add(result.Key, result);
	                        rows.Add(result);
	                    }

	                    foreach (
	                        var valueCalculation in ValueCalculations.Where(v => v.Field == calculatedField))
	                    {
	                        var value = facetValue.GetAggregation(valueCalculation.SummaryMode);
	                        if (value.HasValue)
	                        {
	                            result.Values.Add(valueCalculation.Header,
	                                              facetValue.GetAggregation(valueCalculation.SummaryMode) ?? 0);
	                        }
	                    }

	                }
	            }

	            var columns = new ColumnsModel();

	            columns.Columns.Add(new ColumnDefinition()
	            {
	                Header = "Key",
	                Binding = "Key"
	            });

	            columns.Columns.AddRange(
	                ValueCalculations.Select(
	                    k => new ColumnDefinition() {Header = k.Header, Binding = "Values[" + k.Header + "]"}));

	            Results.AddRange(rows);
	            ResultColumns = columns;

	            var queryEndTime = DateTime.UtcNow.Ticks;

	            ExecutionElapsedTime.Value = new TimeSpan(queryEndTime - queryStartTime);
	        }
	        catch (AggregateException ex)
	        {
	            var badRequest = ex.ExtractSingleInnerException() as BadRequestException;
	            if (badRequest != null)
	            {
	                QueryErrorMessage.Value = badRequest.Message;
	                IsErrorVisible.Value = true;
	            }
	            else
	            {
	                throw;
	            }
	        }
	        catch (TaskCanceledException)
	        {
	        }
	        finally
	        {
	            // there's a bug in silverlight where if a ChildWindow gets closed too soon after it's opened, it leaves the UI
	            // disabled; so delay closing the window by a few milliseconds
	            TaskEx.Delay(TimeSpan.FromMilliseconds(350))
	                  .ContinueOnSuccessInTheUIThread(progressWindow.Close);
	        }
	    }

	    public ColumnsModel ResultColumns
        {
            get { return resultColumns; }
            set { resultColumns = value;
            OnPropertyChanged(() => ResultColumns);}
        }

        public Observable<string> GroupByField { get; private set; } 
        public ObservableCollection<ReportRow> Results { get; private set; } 
	    public BindableCollection<string> DocumentProperties { get; private set; }
		public BindableCollection<string> IndexFields { get; private set; }
        public ObservableCollection<ValueCalculation> ValueCalculations { get; private set; }
        public IList<FacetAggregation> SummaryModes { get { return AllSummaryModes; } } 

	    public ICommand ExportReport
	    {
	        get { return exportReport ?? (exportReport = new ExportReportToCsvCommand(this)); }
	    }

	    public ICommand AddFilter
	    {
	        get { return addFilter ?? (addFilter = new ActionCommand(() => IsFilterVisible.Value = true)); }
	    }

        public ICommand DeleteFilter
        {
            get { return deleteFilter ?? (deleteFilter = new ActionCommand(() =>
            {
                IsFilterVisible.Value = false;
                FilterDoc.SetText("");
            })); }
        }

		public ICommand AddValueCalculation
		{
			get { return new ActionCommand(field => ValueCalculations.Add(new ValueCalculation() {Field  =  (string)field, SummaryMode = FacetAggregation.Count}));}
		}

	    public ICommand DeleteValueCalculation
	    {
            get
            {
                return deleteValueCalculation ??
                       (deleteValueCalculation =
                        new ActionCommand(parameter => ValueCalculations.Remove(parameter as ValueCalculation)));
            }
	    }
	}

    public class ReportRow
    {
        public ReportRow()
        {
            Values = new Dictionary<string, double>();
        }

        public string Key { get; set; }
        public IDictionary<string, double> Values { get; private set; } 
    }

    public class ValueCalculation
    {
        public ValueCalculation()
        {
            
        }

        public string Field { get; set; }

        public FacetAggregation SummaryMode { get; set; }

        public string Header
        {
            get { return SummaryMode.ToString().ToUpper() + " of " + Field; }
        }
    }
}
