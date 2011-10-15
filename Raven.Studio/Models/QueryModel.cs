using System.Collections.Generic;
using System.ComponentModel;
using System.Text.RegularExpressions;
using System.Windows.Input;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class QueryModel : Model
	{
		private readonly string indexName;
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		public const int PageSize = 25;

		private static readonly Regex FieldsFinderRegex = new Regex(@"(^|\s)?([^\s:]+):", RegexOptions.IgnoreCase | RegexOptions.Singleline);
		private readonly List<string> fields = new List<string>();
		private readonly Dictionary<string, List<string>> fieldsTermsDictionary = new Dictionary<string, List<string>>();

		private static string lastQuery;
		private static string lastIndex;

		public QueryModel(string indexName, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.indexName = indexName;
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			DocumentsResult = new Observable<DocumentsModel>();
			Query = new Observable<string>();

			RememberHistory();

			Query.PropertyChanged += GetTermsForUsedFields;
			CompletionProvider = new Observable<ICompletionProvider>();

			GetFields();
			CompletionProvider.Value = new RavenQueryCompletionProvider(fields, fieldsTermsDictionary);
		}

		private void RememberHistory()
		{
			if (lastIndex == indexName)
			{
				Query.Value = lastQuery;
				Execute.Execute(null);
			}
			lastIndex = indexName;
			Query.PropertyChanged += (sender, args) => lastQuery = Query.Value;
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

		private void GetFields()
		{
			asyncDatabaseCommands.GetIndexAsync(indexName)
				.ContinueOnSuccess(definition => fields.AddRange(definition.Fields));
		}

		private void GetTermsForField(string field, List<string> terms)
		{
			asyncDatabaseCommands.GetTermsAsync(IndexName, field, string.Empty, 1024)
				.ContinueOnSuccess(termsFromServer =>
				{
					foreach (var term in termsFromServer)
					{
						if(term.IndexOfAny(new[]{' ','\t'})  == -1)
							terms.Add(term);
						else
							terms.Add('"' + term + '"'); // qoute the term
					}
				});
		}

		public Observable<ICompletionProvider> CompletionProvider { get; private set; }

		public ICommand Execute { get { return new ExecuteQueryCommand(this, asyncDatabaseCommands); } }

		public Observable<string> Query { get; set; }

		public string IndexName
		{
			get { return indexName; }
		}

		private string error;
		public string Error
		{
			get { return error; }
			set { error = value; OnPropertyChanged(); }
		}

		public int CurrentPage
		{
			get { return UrlUtil.GetSkipCount() / PageSize + 1; }
			
		}

		public Observable<DocumentsModel> DocumentsResult { get; private set; }
	}
}