using System.Linq;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class ViewModel : Model
	{
		public ViewModel()
		{
			SetCurrentDatabase();
		}

		protected virtual void Initialize() { }

		public DatabaseModel Database { get; private set; }

		public IAsyncDatabaseCommands DatabaseCommands
		{
			get { return Database.AsyncDatabaseCommands; }
		}

		private void SetCurrentDatabase()
		{
			var applicationModel = ApplicationModel.Current;

			var server = applicationModel.Server;
			if (server.Value == null)
			{
				server.RegisterOnce(SetCurrentDatabase);
				return;
			}

			var databaseName = ApplicationModel.GetQueryParam("database");
			var database = server.Value.Databases.Where(x => x.Name == databaseName).FirstOrDefault();
			if (database != null)
			{
				server.Value.SelectedDatabase.Value = database;
			}
			Database = server.Value.SelectedDatabase.Value;

			Initialize();
		}

		public static string GetParamAfter(string urlPrefix)
		{
			var url = ApplicationModel.Current.NavigationState;
			if (url.StartsWith(urlPrefix) == false)
				return null;

			return url.Substring(urlPrefix.Length);
		}
	}
}