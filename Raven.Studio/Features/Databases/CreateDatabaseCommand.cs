using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Databases
{
	public class CreateDatabaseCommand : Command
	{
		private readonly ServerModel serverModel;
		private readonly IAsyncDatabaseCommands databaseCommands;

		public CreateDatabaseCommand(ServerModel serverModel,IAsyncDatabaseCommands databaseCommands)
		{
			this.serverModel = serverModel;
			this.databaseCommands = databaseCommands;
		}

		public override void Execute(object parameter)
		{
			AskUser.QuestionAsync("Create New Database", "Database name?")
				.ContinueWith(task =>
				{
					if (task.IsCanceled)
						return;

					var databaseName = task.Result;
					if (string.IsNullOrEmpty(databaseName))
						return;

					var addAction = serverModel.Starting("Creating database: " + databaseName);

					databaseCommands.EnsureDatabaseExistsAsync(databaseName)
						.ContinueOnSuccess(() => addAction.Dispose())
						.ContinueOnSuccess(() => EventsBus.Notify(new DatabaseCreated{}))
						.Catch();
				});
		}
	}
}