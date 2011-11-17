using System;
using System.IO;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Features.Databases
{
	public class CreateDatabaseCommand : Command
	{
		private readonly IAsyncDatabaseCommands databaseCommands;

		public CreateDatabaseCommand(IAsyncDatabaseCommands databaseCommands)
		{
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

					if (Path.GetInvalidPathChars().Any(databaseName.Contains))
						throw new ArgumentException("Cannot create a database with invalid path characters: " + databaseName);

					ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
					databaseCommands.EnsureDatabaseExistsAsync(databaseName)
						.ContinueOnSuccess(() => databaseCommands.ForDatabase(databaseName).EnsureSilverlightStartUpAsync())
						.ContinueOnSuccessInTheUIThread(() =>
											{
												ApplicationModel.Current.Server.Value.Databases.Add(new DatabaseModel(databaseName, databaseCommands.ForDatabase(databaseName)));
												ApplicationModel.Current.AddNotification(new Notification("Database " + databaseName + " created"));
											})
						.Catch();
				});
		}
	}
}