using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DatabasesListModel : PageViewModel
	{
		private readonly ChangeDatabaseCommand changeDatabase;

		public DatabasesListModel()
		{
			ModelUrl = "/databases";
			changeDatabase = new ChangeDatabaseCommand();
            Databases = new BindableCollection<DatabaseListItemModel>(m => m.Name);
		}

        public BindableCollection<DatabaseListItemModel> Databases { get; private set; }

        private DatabaseListItemModel selectedDatabase;

        public DatabaseListItemModel SelectedDatabase
		{
			get { return selectedDatabase ?? (selectedDatabase = ApplicationModel.Database.Value != null ? Databases.FirstOrDefault(m => m.Name.Equals(ApplicationModel.Database.Value.Name, StringComparison.InvariantCulture)) : null); }
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
			
		    return TaskEx.WhenAll(
                new[] { ApplicationModel.Current.Server.Value.TimerTickedAsync() } // Fetch databases names from the server
		            .Concat(Databases.Select(m => m.TimerTickedAsync()))); // Refresh statistics
		}

        protected override void OnViewLoaded()
        {
            ApplicationModel.Current.Server.Value.Databases.CollectionChanged += HandleDatabaseListChanged;
            RefreshDatabaseList();
        }

	    private void RefreshDatabaseList()
	    {
            Databases.Match(ApplicationModel.Current.Server.Value.Databases.Select(m => new DatabaseListItemModel(m.Name)).ToArray());
	    }

	    protected override void OnViewUnloaded()
        {
            ApplicationModel.Current.Server.Value.Databases.CollectionChanged -= HandleDatabaseListChanged;
        }

	    private void HandleDatabaseListChanged(object sender, NotifyCollectionChangedEventArgs e)
	    {
	        RefreshDatabaseList();
	    }
	}
}