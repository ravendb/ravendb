using System.Windows.Input;
using Raven.Studio.Features.Databases;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabasesListModel : ViewModel
	{
		public DatabasesListModel()
		{
			ModelUrl = "/databases";
		}

		public BindableCollection<DatabaseModel> Databases
		{
			get { return ApplicationModel.Current.Server.Value.Databases; }
		}

		private DatabaseModel selectedDatabase;
		public DatabaseModel SelectedDatabase
		{
			get { return selectedDatabase ?? (selectedDatabase = ApplicationModel.Current.Server.Value.SelectedDatabase.Value); }
			set
			{
				selectedDatabase = value;
				OnPropertyChanged();
			}
		}

		public ICommand CreateNewDatabase
		{
			get
			{
				return new CreateDatabaseCommand(DatabaseCommands);
			}
		}
	}
}