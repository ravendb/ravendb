using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Extensions;
using System.Reactive.Linq;

namespace Raven.Studio.Models
{
	public class DatabaseSelectionModel : ViewModel
	{
		private ChangeDatabaseCommand changeDatabase;

		public DatabaseSelectionModel()
		{
			changeDatabase = new ChangeDatabaseCommand();
			ApplicationModel.Current.Server.Value.Databases.CollectionChanged += (sender, args) => OnPropertyChanged(() => Databases);
			ApplicationModel.Current.Server.Value.SelectedDatabase.PropertyChanged += (sender, args) => OnPropertyChanged(() => SelectedDatabase);
		}

		public bool SingleTenant
		{
			get { return ApplicationModel.Current.Server.Value.SingleTenant; }
		}

		private bool showSystem = false;
		public IEnumerable<string> Databases
		{
			get
			{
				if(showSystem)
					return ApplicationModel.Current.Server.Value.Databases;
				return ApplicationModel.Current.Server.Value.Databases.Where(s => s != Constants.SystemDatabase);
			}
		}

		public string SelectedDatabase
		{
			get
			{
				var item = ApplicationModel.Database.Value != null ? ApplicationModel.Database.Value.Name : null;
				if (item == Constants.SystemDatabase)
				{
					showSystem = true;
					OnPropertyChanged(() => Databases);
				}
				else if (showSystem)
				{
					showSystem = false;
					OnPropertyChanged(() => Databases);
				}
					
				return item;
			}
			set
			{
				if (changeDatabase.CanExecute(value))
					changeDatabase.Execute(value);
			}
		}

		protected override void OnViewLoaded()
		{
			ApplicationModel.Current.Server.Value.SelectedDatabase
				.ObservePropertyChanged()
				.TakeUntil(Unloaded)
				.Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

			// when the database list changes we need to trigger a refresh of the SelectedDatabase command,
			// but we only want to do it after each batch of changes
			ApplicationModel.Current.Server.Value.Databases.ObserveCollectionChanged()
				.Throttle(TimeSpan.FromMilliseconds(10))
				.ObserveOnDispatcher()
				.TakeUntil(Unloaded)
				.Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

			OnPropertyChanged(() => SelectedDatabase);
		}

		protected override void OnViewUnloaded()
		{
		}
	}
}
