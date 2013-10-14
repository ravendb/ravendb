using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Stats
{
	public class IndexesErrorsStatusSectionModel : StatusSectionModel
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

		public IndexesErrorsStatusSectionModel()
		{
			SectionName = "Index Errors";
			ApplicationModel.Current.Server.Value.RawUrl = null;

			Database.Value.Statistics.PropertyChanged += (sender, args) => OnPropertyChanged(() => Errors);

			var allErrors = Database.Value.Statistics.Value.Errors;
			if (IsShowingErrorForASpecificIndex)
				allErrors = allErrors.Where(e => e.IndexName == indexName).ToArray();
			Errors = allErrors;
		}
	}
}