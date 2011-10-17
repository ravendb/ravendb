using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesErrorsModel : ViewModel
	{
		public BindableCollection<ServerError> Errors { get; private set; }
		
		private string indexName;
		public string IndexName
		{
			get { return indexName; }
			set
			{
				indexName = value;
				OnPropertyChanged();
				OnPropertyChanged("IsShowingErrorForASpecificIndex");
			}
		}

		public bool IsShowingErrorForASpecificIndex
		{
			get { return string.IsNullOrWhiteSpace(IndexName) == false; }
		}

		public IndexesErrorsModel()
		{
			Errors = new BindableCollection<ServerError>(new PrimaryKeyComparer<ServerError>(x => x.Timestamp));

			IndexName = GetParamAfter("/indexes-errors/");
			var errors = Database.Value.Statistics.Value.Errors;
			if (IsShowingErrorForASpecificIndex)
				errors = errors.Where(e => e.Index == IndexName).ToArray();
			Errors.Match(errors);
			Database.Value.Statistics.PropertyChanged += (sender, args) => OnPropertyChanged("Errors");
		}
	}
}