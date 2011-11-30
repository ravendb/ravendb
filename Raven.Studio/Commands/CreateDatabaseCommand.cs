using System;
using System.IO;
using Raven.Client.Extensions;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class CreateDatabaseCommand : Command
	{
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
					DatabaseCommands.EnsureDatabaseExistsAsync(databaseName)
						.ContinueOnSuccess(() => DatabaseCommands.ForDatabase(databaseName).EnsureSilverlightStartUpAsync())
						.ContinueOnSuccessInTheUIThread(() =>
											{
												ApplicationModel.Current.Server.Value.Databases.Add(new DatabaseModel(databaseName, DatabaseCommands.ForDatabase(databaseName)));
												ApplicationModel.Current.AddNotification(new Notification("Database " + databaseName + " created"));
											})
						.Catch();
				});
		}
	}
}