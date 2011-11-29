using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Features.Databases;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabasesListModel : ViewModel
	{
		private readonly ChangeDatabaseCommand changeDatabase;

		public DatabasesListModel()
		{
			ModelUrl = "/databases";
			changeDatabase = new ChangeDatabaseCommand();
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
				if (changeDatabase.CanExecute(selectedDatabase.Name))
					changeDatabase.Execute(selectedDatabase);
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