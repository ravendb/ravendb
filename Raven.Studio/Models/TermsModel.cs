using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text.RegularExpressions;
using System.Windows.Controls;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class TermsModel : PageViewModel
	{

		public TermsModel()
		{
			ModelUrl = "/terms";
			terms = new ObservableCollection<KeyValuePair<string, ObservableCollection<string>>>();
		}

		private string indexName;
		public string IndexName
		{
			get { return indexName; }
			set
			{
				indexName = value;
				OnPropertyChanged(() => IndexName);
			}
		}

		private ObservableCollection<KeyValuePair<string, ObservableCollection<string>>> terms;
		public ObservableCollection<KeyValuePair<string, ObservableCollection<string>>> Terms
		{
			get { return terms; }
			set
			{
				terms = value;
				OnPropertyChanged(() => Terms);
			}
		}

		private readonly BindableCollection<string> fields = new BindableCollection<string>(x => x);
		private readonly Dictionary<string, List<string>> fieldsTermsDictionary = new Dictionary<string, List<string>>();

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);

			IndexName = urlParser.Path.Trim('/');

			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnSuccessInTheUIThread(definition =>
				{
					if (definition == null)
					{
						IndexDefinitionModel.HandleIndexNotFound(IndexName);
						return;
					}
					fields.Match(definition.Fields.OrderBy(f => f).ToList());

					foreach (var field in fields)
					{
						var localterms = fieldsTermsDictionary[field] = new List<string>();
						GetTermsForField(field, localterms);
					}
				}).Catch();
		}

		private void GetTermsForField(string field, List<string> terms)
		{
			DatabaseCommands.GetTermsAsync(IndexName, field, string.Empty, 1024)
				.ContinueOnSuccess(termsFromServer =>
				{
					foreach (var term in termsFromServer)
					{
						if (term.IndexOfAny(new[] { ' ', '\t' }) == -1)
							terms.Add(term);
						else
							terms.Add('"' + term + '"'); // quote the term
					}
				}).ContinueOnSuccessInTheUIThread(() => Terms.Add(new KeyValuePair<string, ObservableCollection<string>>(field, new ObservableCollection<string>(terms))));
		}
	}
}