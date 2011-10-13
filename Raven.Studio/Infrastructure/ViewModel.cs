using System.Linq;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class ViewModel : Model
	{
		public ViewModel()
		{
            Database = new Observable<DatabaseModel>();
			SetCurrentDatabase();
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

			Database.Value = server.Value.SelectedDatabase.Value;
		}
	}
}