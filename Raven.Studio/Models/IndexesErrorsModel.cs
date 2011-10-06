using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesErrorsModel : Model
	{
		public ServerError[] Errors { get; set; }

		public void OnStatisticsUpdated()
		{
			OnPropertyChanged("Errors");
		}
	}
}