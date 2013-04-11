using System.Collections.ObjectModel;
using System.Windows.Input;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class TaskModel : ViewModel
	{
		public TaskModel()
		{
			Sections = new ObservableCollection<TaskSectionModel>();
			SelectedSection = new Observable<TaskSectionModel>();
		}

		public ObservableCollection<TaskSectionModel> Sections { get; private set; }

		public T GetSection<T>() where T : TaskSectionModel
		{
			return Sections.OfType<T>().FirstOrDefault();
		}

		public Observable<TaskSectionModel> SelectedSection { get; private set; }
	}
}