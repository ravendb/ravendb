using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesErrorsModel : PageViewModel
	{
		private ServerError[] errors;
		public ServerError[] Errors
		{
			get { return errors; }
			private set
			{
				errors = value;
				OnPropertyChanged(() => Errors);
			}
		}

		private string indexName;
		public string IndexName
		{
			get { return indexName; }
			set
			{
				indexName = value;
				OnPropertyChanged(() => IndexName);
				OnPropertyChanged(() => IsShowingErrorForASpecificIndex);
			}
		}

		public bool IsShowingErrorForASpecificIndex
		{
			get { return string.IsNullOrWhiteSpace(IndexName) == false; }
		}

		public IndexesErrorsModel()
		{
			ModelUrl = "/indexes-errors";
			Database.Value.Statistics.PropertyChanged += (sender, args) => OnPropertyChanged(() => Errors);
		}

		public override void LoadModelParameters(string parameters)
		{
			IndexName = new UrlParser(parameters).Path.Trim('/');

			var allErrors = Database.Value.Statistics.Value.Errors;
			if (IsShowingErrorForASpecificIndex)
				allErrors = allErrors.Where(e => e.Index == IndexName).ToArray();
			Errors = allErrors;
		}
	}
}