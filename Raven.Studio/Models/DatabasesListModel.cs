using System;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Raven.Studio.Commands;
using Raven.Studio.Extensions;
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

        public DatabaseListItemModel SelectedDatabase
		{
			get { return  ApplicationModel.Database.Value != null ? Databases.FirstOrDefault(m => m.Name.Equals(ApplicationModel.Database.Value.Name, StringComparison.InvariantCulture)) : null; }
			set
			{
				if (value == null)
				{
				    return;
				}

				if (changeDatabase.CanExecute(value.Name))
					changeDatabase.Execute(value.Name);
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
            ApplicationModel.Current.Server.Value.SelectedDatabase
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

            ApplicationModel.Current.Server.Value.Databases.ObserveCollectionChanged()
                .Throttle(TimeSpan.FromMilliseconds(10))
                .ObserveOnDispatcher()
                .TakeUntil(Unloaded)
                .Subscribe(_ => RefreshDatabaseList());

            RefreshDatabaseList();
        }

	    private void RefreshDatabaseList()
	    {
            Databases.Match(ApplicationModel.Current.Server.Value.Databases.Select(name => new DatabaseListItemModel(name)).ToArray());
            OnPropertyChanged(() => SelectedDatabase);
	    }
	}
}