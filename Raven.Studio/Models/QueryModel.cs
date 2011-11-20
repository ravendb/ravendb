using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows.Input;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Studio.Commands;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class QueryModel : ViewModel
	{
		
		#region SpatialQuery

		private bool isSpatialQuerySupported;
		public bool IsSpatialQuerySupported
		{
			get { return isSpatialQuerySupported; }
			set
			{
				isSpatialQuerySupported = value;
				OnPropertyChanged();
			}
		}

		private bool isSpatialQuery;
		public bool IsSpatialQuery
		{
			get { return isSpatialQuery; }
			set
			{
				isSpatialQuery = value;
				OnPropertyChanged();
			}
		}

		private double? latitude;
		public double? Latitude
		{
			get { return latitude; }
			set
			{
				latitude = value;
				OnPropertyChanged();
			}
		}

		private double? longitude;
		public double? Longitude
		{
			get { return longitude; }
			set
			{
				longitude = value;
				OnPropertyChanged();
			}
		}

		private double? radius;
		public double? Radius
		{
			get { return radius; }
			set
			{
				radius = value;
				OnPropertyChanged();
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
				OnPropertyChanged();
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
				set { this.value = value; OnPropertyChanged();}
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

		private static readonly Regex FieldsFinderRegex = new Regex(@"(^|\s)?([^\s:]+):", RegexOptions.IgnoreCase | RegexOptions.Singleline);

		private readonly BindableCollection<string> fields = new BindableCollection<string>(x => x);
		private readonly Dictionary<string, List<string>> fieldsTermsDictionary = new Dictionary<string, List<string>>();

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

			Query.PropertyChanged += GetTermsForUsedFields;
			CompletionProvider = new Observable<ICompletionProvider>();
			CompletionProvider.Value = new RavenQueryCompletionProvider(fields, fieldsTermsDictionary);
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			IndexName = urlParser.Path.Trim('/');
			Pager.SetSkip(urlParser);

			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnSuccessInTheUIThread(definition =>
				{
					if (definition == null)
					{
						UrlUtil.Navigate("/NotFound?indexName=" + IndexName);
						return;
					}
					fields.Match(definition.Fields);
					IsSpatialQuerySupported = definition.Map.Contains("SpatialIndex.Generate");
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
			if (IndexName == null || lastIndex != IndexName)
				return;

			Query.Value = lastQuery;
			Execute.Execute(null);
		}

		private void GetTermsForUsedFields(object sender, PropertyChangedEventArgs e)
		{
			var text = ((Observable<string>)sender).Value;
			if (string.IsNullOrEmpty(text))
				return;

			var matches = FieldsFinderRegex.Matches(text);
			foreach (Match match in matches)
			{
				var field = match.Groups[2].Value;
				if (fieldsTermsDictionary.ContainsKey(field))
					continue;
				var terms = fieldsTermsDictionary[field] = new List<string>();
				GetTermsForField(field, terms);
			}
		}

		private void GetTermsForField(string field, List<string> terms)
		{
			DatabaseCommands.GetTermsAsync(IndexName, field, string.Empty, 1024)
				.ContinueOnSuccess(termsFromServer =>
				{
					foreach (var term in termsFromServer)
					{
						if(term.IndexOfAny(new[]{' ','\t'})  == -1)
							terms.Add(term);
						else
							terms.Add('"' + term + '"'); // quote the term
					}
				});
		}

		public Observable<ICompletionProvider> CompletionProvider { get; private set; }

		public ICommand Execute { get { return new ExecuteQueryCommand(this, DatabaseCommands); } }

		public Observable<string> Query { get; set; }

		private string error;
		public string Error
		{
			get { return error; }
			set { error = value; OnPropertyChanged(); }
		}

		public readonly PagerModel Pager = new PagerModel();

		public Observable<DocumentsModel> DocumentsResult { get; private set; }

		public string ViewTitle
		{
			get { return "Query: " + IndexName; }
		}

		public BindableCollection<FieldAndTerm> Suggestions { get; private set; }
	}
}