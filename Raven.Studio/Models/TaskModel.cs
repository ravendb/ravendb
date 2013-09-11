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
			Sections = new ObservableCollection<ViewModel>();
            SelectedSection = new Observable<ViewModel>();
		}

        public ObservableCollection<ViewModel> Sections { get; private set; }

        public T GetSection<T>() where T : ViewModel
		{
			return Sections.OfType<T>().FirstOrDefault();
		}

        public Observable<ViewModel> SelectedSection { get; private set; }
	}
}