using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Data;
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
			DatabasesWithEditableBundles = new List<string>();
			Databases = new BindableCollection<DatabaseListItemModel>(m => m.Name);
			Databases.CollectionChanged += (sender, args) => UpdateBundlesList();
			ApplicationModel.Current.Server.Value.SelectedDatabase.PropertyChanged += (sender, args) =>
			{
				OnPropertyChanged(() => SelectedDatabase);
				OnPropertyChanged(() => ShowEditBundles);
			};
		}

		private void UpdateBundlesList()
		{
			foreach (var databaseListItemModel in Databases)
			{
				ApplicationModel.Current.Server.Value.DocumentStore
					.AsyncDatabaseCommands
					.ForDefaultDatabase()
					.GetAsync("Raven/Databases/" + databaseListItemModel.Name)
					.ContinueOnSuccessInTheUIThread(doc =>
					{
						if (doc == null || doc.DataAsJson["Settings"].SelectToken("Raven/ActiveBundles", false) == null || DatabasesWithEditableBundles.Contains(doc.Key.Split('/')[2])) 
							return;

						var bundles = doc.DataAsJson["Settings"].SelectToken("Raven/ActiveBundles", false).ToString();
						if (!bundles.Contains("Quotas") && !bundles.Contains("Replication") && !bundles.Contains("Versioning")) 
							return;

						DatabasesWithEditableBundles.Add(doc.Key.Split('/')[2]);
						OnPropertyChanged(() => ShowEditBundles);
					});
			}
		}

		public BindableCollection<DatabaseListItemModel> Databases { get; private set; }
		public List<string> DatabasesWithEditableBundles { get; set; }

		public DatabaseListItemModel SelectedDatabase
		{
			get { return ApplicationModel.Database.Value != null ? Databases.FirstOrDefault(m => m.Name.Equals(ApplicationModel.Database.Value.Name, StringComparison.InvariantCulture)) : null; }
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

		public bool ShowEditBundles
		{
			get
			{
				return SelectedDatabase != null && DatabasesWithEditableBundles.Contains(SelectedDatabase.Name);
			}
		}

		public ICommand DeleteSelectedDatabase
		{
			get { return new DeleteDatabaseCommand(this); }
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
			Databases.Match(ApplicationModel.Current.Server.Value.Databases.Where(s => s != Constants.SystemDatabase).Select(name => new DatabaseListItemModel(name)).ToArray());
			OnPropertyChanged(() => SelectedDatabase);
		}
	}
}