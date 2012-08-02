using System;
using System.Collections.Generic;
using System.IO;
using System.Windows.Controls;
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
							var bundlesSettings = new List<ChildWindow>();
							if(bundles.Encryption.IsChecked == true)
								bundlesSettings.Add(new EncryotionSettings());
							if(bundles.Quotas.IsChecked == true)
								bundlesSettings.Add(new QuotasSettings());
							if(bundles.Replication.IsChecked == true)
								bundlesSettings.Add(new ReplicationSettings());
							if(bundles.Versioning.IsChecked == true)
								bundlesSettings.Add(new VersioningSettings());

							new Wizard(bundlesSettings).StartAsync()
								.ContinueOnSuccessInTheUIThread(bundlesData =>
								{
									ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
									var settings = UpdateSettings(newDatabase, bundles, bundlesData);

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
						});
				})
				.Catch();
		}

		private static Dictionary<string, string> UpdateSettings(NewDatabase newDatabase, BundlesSelect bundles, List<ChildWindow> bundlesData)
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

			if(bundles.Bundles.Contains("Encryption"))
			{
				var encryptionData = bundlesData.FirstOrDefault(window => window is EncryotionSettings) as EncryotionSettings;
				if(encryptionData != null)
				{
					//TODO: update settings
				}
			}

			if (bundles.Bundles.Contains("Quotas"))
			{
				var quatasData = bundlesData.FirstOrDefault(window => window is QuotasSettings) as QuotasSettings;
				if (quatasData != null)
				{
					//TODO: update settings
				}
			}

			if (bundles.Bundles.Contains("Replication"))
			{
				var replicationData = bundlesData.FirstOrDefault(window => window is ReplicationSettings) as ReplicationSettings;
				if (replicationData != null)
				{
					//TODO: update settings
				}
			}

			if (bundles.Bundles.Contains("Versioning"))
			{
				var versioningData = bundlesData.FirstOrDefault(window => window is VersioningSettings) as VersioningSettings;
				if (versioningData != null)
				{
					//TODO: update settings
				}
			}
			return settings;
		}
	}
}
