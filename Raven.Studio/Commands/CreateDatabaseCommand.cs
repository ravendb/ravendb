using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Studio.Controls;
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
			var newDatabase = new NewDatabase();
			newDatabase.Show();
			newDatabase.Closed += (sender, args) =>
			{
				if (newDatabase.DialogResult != null && !newDatabase.DialogResult.Value)
					return;

				var databaseName = newDatabase.DbName.Text;
				if (string.IsNullOrEmpty(databaseName))
					return;

				if (Path.GetInvalidPathChars().Any(databaseName.Contains))
					throw new ArgumentException("Cannot create a database with invalid path characters: " + databaseName);

				var bundles = new BundlesSelect();
				bundles.Show();
				bundles.Closed += (o, eventArgs) =>
				{
					var selectedBundles = bundles.Bundles.Aggregate("", (current, bundle) => current + (bundle + ";"));
					
					ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
					try
					{
						DatabaseDocument databaseDocuemnt;
						if (newDatabase.ShowAdvande.IsChecked != null && newDatabase.ShowAdvande.IsChecked.Value)
						{
							databaseDocuemnt = new DatabaseDocument
							{
								Id = newDatabase.DbName.Text,
								Settings =
								{
									{"Raven/DataDir", Path.Combine("~", Path.Combine(newDatabase.DbPath.Text, databaseName))},
									{"Raven/Esent/LogsPath", Path.Combine("~", Path.Combine(newDatabase.LogsPath.Text, databaseName))},
									{"Raven/IndexStoragePath", Path.Combine("~", Path.Combine(newDatabase.IndexPath.Text, databaseName))},
									{"Raven/ActiveBundles", selectedBundles}
								}
							};
						}
						else
						{
							databaseDocuemnt = new DatabaseDocument
							{
								Id = newDatabase.DbName.Text,
								Settings =
								{
									{"Raven/DataDir", Path.Combine("~", Path.Combine("Databases", databaseName))},
									{"Raven/ActiveBundles", selectedBundles}
								}
							};
						}

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
					}
					catch (Exception ex)
					{
						Infrastructure.Execute.OnTheUI(() => ApplicationModel.Current.AddErrorNotification(ex));
					}
				};
				};	
		}
	}
}
