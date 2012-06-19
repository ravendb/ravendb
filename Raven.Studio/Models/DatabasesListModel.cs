using System.Threading.Tasks;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabasesListModel : PageViewModel
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
			get { return selectedDatabase ?? (selectedDatabase = ApplicationModel.Database.Value); }
			set
			{
				selectedDatabase = value;
				OnPropertyChanged(() => SelectedDatabase);
				if (changeDatabase.CanExecute(selectedDatabase.Name))
					changeDatabase.Execute(selectedDatabase);
			}
		}

		public override Task TimerTickedAsync()
		{
			// Fetch databases names from the server
			return ApplicationModel.Current.Server.Value.TimerTickedAsync();
		}
	}
}