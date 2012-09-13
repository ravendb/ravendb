using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class SettingsModel : ViewModel
	{
		public bool Creation { get; set; }
		public DatabaseDocument DatabaseDocument { get; set; }

		public SettingsModel() 
		{
            Sections = new ObservableCollection<SettingsSectionModel>();
            SelectedSection = new Observable<SettingsSectionModel>();
		}

        public ObservableCollection<SettingsSectionModel> Sections { get; private set; }

        public T GetSection<T>() where T : SettingsSectionModel
        {
            return Sections.OfType<T>().FirstOrDefault();
        }

		public string CurrentDatabase { get { return ApplicationModel.Database.Value.Name; } }

        public Observable<SettingsSectionModel> SelectedSection { get; private set; }
		
		private ICommand _saveBundlesCommand;
		public ICommand SaveBundles { get { return _saveBundlesCommand ?? (_saveBundlesCommand = new SaveBundlesCommand(this)); } }
	}
}