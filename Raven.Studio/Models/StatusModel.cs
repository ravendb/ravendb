using System.Collections.ObjectModel;
using System.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class StatusModel : ViewModel
	{
		public StatusModel()
		{
			Sections = new ObservableCollection<StatusSectionModel>();
			SelectedSection = new Observable<StatusSectionModel>();
		}

		public ObservableCollection<StatusSectionModel> Sections { get; private set; }

		public T GetSection<T>() where T : StatusSectionModel
		{
			return Sections.OfType<T>().FirstOrDefault();
		}

		public Observable<StatusSectionModel> SelectedSection { get; private set; }
	}
}
