using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Studio.Controls;
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
			new NewDatabase().ShowAsync()
				.ContinueOnSuccessInTheUIThread(newDatabase =>
				{
					var databaseName = newDatabase.DbName.Text;
					if (string.IsNullOrEmpty(databaseName))
						return;

					if (Path.GetInvalidPathChars().Any(databaseName.Contains))
						throw new ArgumentException("Cannot create a database with invalid path characters: " + databaseName);
					if(ApplicationModel.Current.Server.Value.Databases.Count(s => s == databaseName) != 0)
						throw new ArgumentException("A database with the name " + databaseName + " already exists");

					new BundlesSelect().ShowAsync()
						.ContinueOnSuccessInTheUIThread(bundles =>
						{
							ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
							var settings = UpdateSettings(newDatabase, bundles);

							var databaseDocuemnt = new DatabaseDocument
							{
								Id = newDatabase.DbName.Text,
								Settings = settings
							};

							DatabaseCommands.CreateDatabaseAsync(databaseDocuemnt).ContinueOnSuccess(
								() => DatabaseCommands.ForDatabase(databaseName).EnsureSilverlightStartUpAsync())
								.ContinueOnSuccessInTheUIThread(() =>
								{
									var model = parameter as DatabasesListModel;
									if (model != null)
										model.ForceTimerTicked();
									ApplicationModel.Current.AddNotification(
										new Notification("Database " + databaseName + " created"));
									Command.ExecuteCommand(new ChangeDatabaseCommand(), databaseName);
								})
								.Catch();
						});
				})
				.Catch();
		}

		private static Dictionary<string, string> UpdateSettings(NewDatabase newDatabase, BundlesSelect bundles)
		{
			var settings = new Dictionary<string, string>
			{
				{
					"Raven/DataDir", newDatabase.ShowAdvanded.IsChecked == true
					                 	? newDatabase.DbPath.Text
					                 	: Path.Combine("~", Path.Combine("Databases", newDatabase.DbName.Text))
					},
				{"Raven/ActiveBundles", string.Join(";", bundles.Bundles)}
			};

			if (!string.IsNullOrWhiteSpace(newDatabase.LogsPath.Text))
				settings.Add("Raven/Esent/LogsPath", newDatabase.LogsPath.Text);
			if (!string.IsNullOrWhiteSpace(newDatabase.IndexPath.Text))
				settings.Add("Raven/IndexStoragePath", newDatabase.IndexPath.Text);
			return settings;
		}
	}
}
