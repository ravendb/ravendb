using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Linq;
using Raven.Studio.Controls;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
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

		public ReportingModel()
		{
			PageTitle = "Reporting";
			ModelUrl = "/reporting";
			AvailableIndexes = new BindableCollection<string>(x => x);
			IndexFields = new BindableCollection<string>(x => x);
            Results = new ObservableCollection<ReportRow>();
            GroupByField = new Observable<string>();
            ValueCalculations = new ObservableCollection<ValueCalculation>();
		}

		private string indexName;
	    private ColumnsModel resultColumns;
	    private ICommand deleteValueCalculation;
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

			AvailableIndexes.Match(Database.Value.Statistics.Value.Indexes.Select(i => i.Name).ToArray());
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);

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

                    IndexFields.Match(task.Result.Fields);
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
	        if (string.IsNullOrEmpty(GroupByField.Value))
	        {
	            ApplicationModel.Current.Notifications.Add(new Notification("You must select a field to group by"));
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
	            IsIndeterminate = true,
	            CanCancel = true
	        };

	        progressWindow.Closed += delegate { cancelationTokenSource.Cancel(); };
	        progressWindow.Show();

	        try
	        {
	            var queryFacetsTask = DatabaseCommands.GetFacetsAsync(IndexName, new IndexQuery(),
	                                                                  AggregationQuery.GetFacets(facets), 0, 512);

	            await TaskEx.WhenAny(
	                queryFacetsTask,
	                TaskEx.Delay(int.MaxValue, cancelationTokenSource.Token));

                if (cancelationTokenSource.IsCancellationRequested)
                {
                    return;
                }

	            var facetResults = await queryFacetsTask;

	            if (facetResults.Results.Count < 1)
	            {
	                return;
	            }

	            var rowsByKey = new Dictionary<string, ReportRow>();

	            foreach (var facetResult in facetResults.Results)
	            {
	                foreach (var facetValue in facetResult.Value.Values)
	                {
	                    ReportRow result;
	                    if (!rowsByKey.TryGetValue(facetValue.Range, out result))
	                    {
	                        result = new ReportRow {Key = facetValue.Range};
                            rowsByKey.Add(result.Key, result);
                            Results.Add(result);
	                    }

	                    foreach (
	                        var facetAggregation in AllSummaryModes)
	                    {
                            var value = facetValue.GetAggregation(facetAggregation);
	                        if (value.HasValue)
	                        {
	                            result.Values.Add(facetAggregation.ToString().ToUpper() + " of " + facetResult.Key,
	                                              value.Value);
	                        }
	                    }

	                }
	            }

	            var keys = Results.SelectMany(r => r.Values.Keys).Distinct();

	            var columns = new ColumnsModel();

	            columns.Columns.Add(new ColumnDefinition()
	            {
	                Header = "Key",
	                Binding = "Key"
	            });

	            columns.Columns.AddRange(
	                keys.Select(k => new ColumnDefinition() {Header = k, Binding = "Values[" + k + "]"}));

	            ResultColumns = columns;
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

		public ICommand AddValueCalculation
		{
			get { return new ActionCommand(field => ValueCalculations.Add(new ValueCalculation() {Field  =  (string)field, SummaryMode = FacetAggregation.Sum}));}
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
    }
}
