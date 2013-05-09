using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class ReportingModel : PageViewModel, IHasPageTitle
	{
		public string PageTitle { get; private set; }
		private const string CollectionsIndex = "Raven/DocumentsByEntityName";

		public ReportingModel()
		{
			PageTitle = "Reporting";
			ModelUrl = "/reporting";
			AvailableIndexes = new BindableCollection<string>(x => x);
			AvailableCollections = new BindableCollection<string>(s => s);
			ParamsForSelectedCollection = new ObservableCollection<string>();
			Aggregations = new BindableCollection<AggregationData>(data => data);
            Results = new ObservableCollection<ReportRow>();
		}

		private string selectedCollection;
		public string SelectedCollection
		{
			get { return selectedCollection; }
			set
			{
				if (string.IsNullOrWhiteSpace(value))
					return;


				selectedCollection = value;
				UpdateProperties();
				Aggregations.Clear();
				OnPropertyChanged(() => Aggregations);
				OnPropertyChanged(() => SelectedCollection);
			}
		}

		private string indexName;
	    private ColumnsModel resultColumns;
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

		private void UpdateAvailableCollections()
		{
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
				CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccessInTheUIThread(collections =>
				{
					AvailableCollections.Clear();
					AvailableCollections.AddRange(collections.OrderByDescending(x => x.Count)
												  .Where(x => x.Count > 0)
												  .Select(col => col.Name).ToList());

					OnPropertyChanged(() => AvailableCollections);
				});
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

			UpdateAvailableIndexes();
			UpdateAvailableCollections();

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
					if (!task.IsFaulted && task.Result != null) 
						return;

					if (AvailableIndexes.Any())
						NavigateToIndexReport(AvailableIndexes.FirstOrDefault());
					else
						NavigateToIndexesList();
				}).Catch();
		}

		private void UpdateProperties()
		{
			DatabaseCommands.GetIndexAsync(IndexName).ContinueOnSuccessInTheUIThread(index =>
			{
				var matchingProperties = index.Fields;

				ParamsForSelectedCollection = new ObservableCollection<string>(matchingProperties);
			});	
		}

		public ICommand DeleteAggregationCommand{get{return new ActionCommand(DeleteAggregation);}}

		private void DeleteAggregation(object parameter)
		{
			var data = parameter as AggregationData;
			Aggregations.Remove(data);

			OnPropertyChanged(() => Aggregations);
		}

		public ICommand ExecuteReportCommand{get{return new AsyncActionCommand(ExecuteReport);}}

	    private async Task ExecuteReport()
	    {
	        var data = Aggregations.Where(aggregationData => aggregationData.HasData()).ToList();
	        if (data.Count == 0)
	        {
	            ApplicationModel.Current.Notifications.Add(new Notification("No Aggregation with valid data found"));
	            return;
	        }

	        var facets = new List<AggregationQuery>();

	        foreach (var aggregationData in data)
	        {
	            facets.Add(new AggregationQuery
	            {
	                Name = aggregationData.AggregateOn,
	                AggregationField = aggregationData.CalculateOn,
	                Aggregation = aggregationData.FacetAggregation
	            });
	        }

            Results.Clear();

	        var facetResults =
	            await
	            DatabaseCommands.GetFacetsAsync(IndexName, new IndexQuery(), AggregationQuery.GetFacets(facets), 0, 512);

            if (facetResults.Results.Count < 1)
            {
                return;
            }

	        var facetResult = facetResults.Results.FirstOrDefault().Value;
	        var aggregation = data[0];

	        foreach (var facetValue in facetResult.Values)
	        {
	            var result = new ReportRow {Key = facetValue.Range};

                foreach (var facetAggregation in new[] { FacetAggregation.Count, FacetAggregation.Min, FacetAggregation.Max, FacetAggregation.Sum, FacetAggregation.Average})
	            {
	                if ((aggregation.FacetAggregation & facetAggregation) == facetAggregation)
	                {
                        result.Values.Add(facetAggregation.ToString().ToUpper() + " of " + aggregation.CalculateOn, facetValue.GetAggregation(facetAggregation) ?? 0);
	                }
	            }

	            Results.Add(result);
	        }

	        var keys = Results.SelectMany(r => r.Values.Keys).Distinct();

	        var columns = new ColumnsModel();

            columns.Columns.Add(new ColumnDefinition()
            {
                Header = "Key", Binding = "Key"
            });

            columns.Columns.AddRange(keys.Select(k => new ColumnDefinition() { Header = k, Binding = "Values[" + k + "]"}));

	        ResultColumns = columns;
	    }

	    public ColumnsModel ResultColumns
        {
            get { return resultColumns; }
            set { resultColumns = value;
            OnPropertyChanged(() => ResultColumns);}
        }

        public ObservableCollection<ReportRow> Results { get; private set; } 
	    public BindableCollection<string> AvailableCollections { get; set; }
		public ObservableCollection<string> ParamsForSelectedCollection { get; set; }
		public BindableCollection<AggregationData> Aggregations { get; set; }
		public ICommand AddAggregation
		{
			get { return new ActionCommand(() => Aggregations.Add(new AggregationData()));}
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
}
