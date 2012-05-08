using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Input;
using Raven.Client.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class QueryModel : ViewModel, IHasPageTitle
	{
		private QueryIndexAutoComplete queryIndexAutoComplete;
		public QueryIndexAutoComplete QueryIndexAutoComplete
		{
			get { return queryIndexAutoComplete; }
			set
			{
				queryIndexAutoComplete = value;
				OnPropertyChanged(() => QueryIndexAutoComplete);
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
				OnPropertyChanged(() => IsSpatialQuerySupported);
			}
		}

		private bool isSpatialQuery;
		public bool IsSpatialQuery
		{
			get { return isSpatialQuery; }
			set
			{
				isSpatialQuery = value;
				OnPropertyChanged(() => IsSpatialQuery);
			}
		}

		private double? latitude;
		public double? Latitude
		{
			get { return latitude; }
			set
			{
				latitude = value;
				OnPropertyChanged(() => Latitude);
			}
		}

		private double? longitude;
		public double? Longitude
		{
			get { return longitude; }
			set
			{
				longitude = value;
				OnPropertyChanged(() => Longitude);
			}
		}

		private double? radius;
		public double? Radius
		{
			get { return radius; }
			set
			{
				radius = value;
				OnPropertyChanged(() => Radius);
			}
		}

		#endregion

		private string indexName;
		public string IndexName
		{
			get
			{
				return indexName;
			}
			private set
			{
				if (string.IsNullOrWhiteSpace(value))
				{
					UrlUtil.Navigate("/indexes");
				}

				indexName = value;
				OnPropertyChanged(() => IndexName);
				RestoreHistory();
			}
		}

		#region Sorting

		public const string SortByDescSuffix = " DESC";

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
			get { return new ChangeFieldValueCommand<QueryModel>(this, x => x.SortBy.Add(new StringRef { Value = SortByOptions.First() })); }
		}

		public ICommand RemoveSortBy
		{
			get { return new RemoveSortByCommand(this); }
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
				StringRef firstOrDefault = model.SortBy.FirstOrDefault(x => x.Value == field);
				if (firstOrDefault != null)
					model.SortBy.Remove(firstOrDefault);
			}
		}

		private void SetSortByOptions(ICollection<string> items)
		{
			foreach (var item in items)
			{
				SortByOptions.Add(item);
				SortByOptions.Add(item + SortByDescSuffix);
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
				OnPropertyChanged(() => DynamicSelectedOption);
			}
		}

		private static string lastQuery;
		private static string lastIndex;

		public QueryModel()
		{
			ModelUrl = "/query";
			
			DocumentsResult = new Observable<DocumentsModel>();
			Query = new Observable<string>();

			SortBy = new BindableCollection<StringRef>(x => x.Value);
			SortByOptions = new BindableCollection<string>(x => x);
			Suggestions = new BindableCollection<FieldAndTerm>(x => x.Field);
			DynamicOptions = new BindableCollection<string>(x => x) {"AllDocs"};
			DynamicSelectedOption = DynamicOptions[0];
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);

			if (urlParser.GetQueryParam("mode") == "dynamic")
			{
				IsDynamicQuery = true;
				DatabaseCommands.GetTermsAsync("Raven/DocumentsByEntityName", "Tag", "", 100)
					.ContinueOnSuccess(collections => DynamicOptions.Match(new[] { "AllDocs" }.Concat(collections).ToArray()));
				return;
			}

			IndexName = urlParser.Path.Trim('/');

			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnSuccessInTheUIThread(definition =>
				{
					if (definition == null)
					{
						IndexDefinitionModel.HandleIndexNotFound(IndexName);
						return;
					}
					var fields = definition.Fields;
					QueryIndexAutoComplete = new QueryIndexAutoComplete(IndexName, Query, fields);
					
					const string spatialindexGenerate = "SpatialIndex.Generate";
					IsSpatialQuerySupported =
						definition.Maps.Any(x => x.Contains(spatialindexGenerate)) ||
						(definition.Reduce != null && definition.Reduce.Contains(spatialindexGenerate));

					SetSortByOptions(fields);
					Execute.Execute(string.Empty);
				}).Catch();
		}

		public void RememberHistory()
		{
			lastIndex = IndexName;
			lastQuery = Query.Value;
		}

		public void RestoreHistory()
		{
			if (IndexName == null || lastIndex != IndexName || string.IsNullOrWhiteSpace(lastQuery))
				return;

			Query.Value = lastQuery;
			Execute.Execute(null);
		}

		public ICommand Execute { get { return new ExecuteQueryCommand(this); } }

		public Observable<string> Query { get; set; }

		private TimeSpan queryTime;
		public TimeSpan QueryTime
		{
			get { return queryTime; }
			set
			{
				queryTime = value;
				OnPropertyChanged(() => QueryTime);
			}
		}
		private RavenQueryStatistics results;
		public RavenQueryStatistics Results
		{
			get { return results; }
			set
			{
				results = value;
				OnPropertyChanged(() => Results);
			}
		}

		private string error;
		public string Error
		{
			get { return error; }
			set { error = value; OnPropertyChanged(() => Error); }
		}

		public Observable<DocumentsModel> DocumentsResult { get; private set; }

		public BindableCollection<FieldAndTerm> Suggestions { get; private set; }
		public ICommand RepairTermInQuery
		{
			get { return new RepairTermInQueryCommand(this); }
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
				model.Query.Value = model.Query.Value.Replace(fieldAndTerm.Term, fieldAndTerm.SuggestedTerm);
				model.Execute.Execute(null);
			}
		}

		public string PageTitle
		{
			get { return "Query Index"; }
		}
	}
}