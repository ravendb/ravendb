using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class ReportingModel : PageViewModel, IHasPageTitle
	{
		public string PageTitle { get; private set; }
		private const string CollectionsIndex = "Raven/DocumentsByEntityName";

		public ReportingModel()
		{
			ModelUrl = "/reporting";
			AvailableIndexes = new BindableCollection<string>(x => x);
			AvailableCollections = new BindableCollection<string>(s => s);
			ParamsForSelectedCollection = new ObservableCollection<string>();
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

				OnPropertyChanged(() => SelectedCollection);
			}
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

		public BindableCollection<string> AvailableCollections { get; set; }
		public ObservableCollection<string> ParamsForSelectedCollection { get; set; }
		public BindableCollection<AggregationData> Aggregations { get; set; } 
	}

	public class AggregationData
	{
		public string AggregateOn { get; set; }
		public FacetAggregation FacetAggregation
		{
			get
			{
				var result = FacetAggregation.None;

				if(Max)
					result |= FacetAggregation.Max;
				if(Min)
					result |= FacetAggregation.Min;
				if (Average)
					result |= FacetAggregation.Average;
				if (Count)
					result |= FacetAggregation.Count; 
				if (Sum)
					result |= FacetAggregation.Sum;

				return result;
			}
		}

		public bool Max { get; set; }
		public bool Min { get; set; }
		public bool Count { get; set; }
		public bool Average { get; set; }
		public bool Sum { get; set; }
	}
}
