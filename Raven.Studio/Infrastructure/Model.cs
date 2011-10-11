using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class Model : NotifyPropertyChangedBase
	{
		public string NavigationState { get; set; }

		private IAsyncDatabaseCommands databaseCommands;
		public IAsyncDatabaseCommands DatabaseCommands
		{
			get { return databaseCommands ?? (databaseCommands = GetDatabaseCommands()); }
		}

		private IAsyncDatabaseCommands GetDatabaseCommands()
		{
			var server = ApplicationModel.Current.Server;
			if (server.Value == null)
			{
				server.RegisterOnce(() => GetDatabaseCommands());
				return null;
			}

			var database = SetCurrentDatabase(server.Value);
			return database.AsyncDatabaseCommands;
		}

		private DatabaseModel SetCurrentDatabase(ServerModel server)
		{
			var databaseName = ApplicationModel.GetQueryParam("database");
			var database = server.Databases.Where(x => x.Name == databaseName).FirstOrDefault();
			if (database != null)
			{
				server.SelectedDatabase.Value = database;
			}
			return server.SelectedDatabase.Value;
		}

		private Task currentTask;
		private DateTime lastRefresh;
		protected TimeSpan RefreshRate { get; set; }

		protected Model()
		{
			RefreshRate = TimeSpan.FromSeconds(5);
		}

		internal void ForceTimerTicked()
		{
			lastRefresh = DateTime.MinValue;
			TimerTicked();
		}

		internal void TimerTicked()
		{
			if (currentTask != null)
				return;

			lock (this)
			{
				if (currentTask != null)
					return;

				if (DateTime.Now - lastRefresh < GetRefreshRate())
					return;

				currentTask = TimerTickedAsync();

				if (currentTask == null)
					return;

				currentTask
					.Catch()
					.Finally(() =>
					{
						lastRefresh = DateTime.Now;
						currentTask = null;
					});
			}
		}

		private TimeSpan GetRefreshRate()
		{
			if (Debugger.IsAttached)
				return RefreshRate.Add(TimeSpan.FromMinutes(5));
			return RefreshRate;
		}

		protected virtual Task TimerTickedAsync()
		{
			return null;
		}
	}
}