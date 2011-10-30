using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Interop;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class ViewModel : Model
	{
		public List<string> ModelUrlIgnoreList { get; private set; }
		public string ModelUrl { get; set; }
		public bool IsLoaded { get; private set; }

		public ViewModel()
		{
			ModelUrlIgnoreList = new List<string>();
			Database = new Observable<DatabaseModel>();
			SetCurrentDatabase();
		}

		public void LoadModel(string state)
		{
			if (string.IsNullOrWhiteSpace(state) == false &&
				state.StartsWith(ModelUrl, StringComparison.InvariantCultureIgnoreCase) &&
				ModelUrlIgnoreList.Any(state.StartsWith) == false)
			{
				LoadModelParameters(GetParamAfter(ModelUrl, state));
			}
			IsLoaded = true;
		}

		public virtual void LoadModelParameters(string parameters) { }

		protected override Task TimerTickedAsync()
		{
			return IsLoaded == false ? null : LoadedTimerTickedAsync();
		}

		protected virtual Task LoadedTimerTickedAsync()
		{
			return null;
		}

		public Observable<DatabaseModel> Database { get; private set; }

		public IAsyncDatabaseCommands DatabaseCommands
		{
			get { return Database.Value.AsyncDatabaseCommands; }
		}

		private void SetCurrentDatabase()
		{
			var applicationModel = ApplicationModel.Current;

			var server = applicationModel.Server;
			// TODO: Due a recent change, Server is not null on startup
			if (server.Value == null)
			{
				server.RegisterOnce(SetCurrentDatabase);
				return;
			}

			var databaseName = new UrlUtil().GetQueryParam("database");
			var database = server.Value.Databases.Where(x => x.Name == databaseName).FirstOrDefault();
			if (database != null)
			{
				server.Value.SelectedDatabase.Value = database;
			}

			Database.Value = server.Value.SelectedDatabase.Value;
		}

		public static string GetParamAfter(string urlPrefix)
		{
			return GetParamAfter(urlPrefix, UrlUtil.Url);
		}

		public static string GetParamAfter(string urlPrefix, string state)
		{
			var url = state;
			if (url.StartsWith(urlPrefix) == false)
				return null;

			return url.Substring(urlPrefix.Length);
		}
	}
}