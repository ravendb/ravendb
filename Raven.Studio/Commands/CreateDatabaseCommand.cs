using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Windows.Controls;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Client;
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
					if (ApplicationModel.Current.Server.Value.Databases.Count(s => s == databaseName) != 0)
						throw new ArgumentException("A database with the name " + databaseName + " already exists");

					new BundlesSelect().ShowAsync()
						.ContinueOnSuccessInTheUIThread(bundles =>
						{
							var bundlesSettings = new List<ChildWindow>();
							if (bundles.Encryption.IsChecked == true)
								bundlesSettings.Add(new EncryotionSettings());
							if (bundles.Quotas.IsChecked == true)
								bundlesSettings.Add(new QuotasSettings());
							if (bundles.Replication.IsChecked == true)
								bundlesSettings.Add(new ReplicationSettings());
							if (bundles.Versioning.IsChecked == true)
								bundlesSettings.Add(new VersioningSettings());

							new Wizard(bundlesSettings).StartAsync()
								.ContinueOnSuccessInTheUIThread(bundlesData =>
								{
									ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
									var settings = UpdateSettings(newDatabase, bundles, bundlesData);
									var securedSettings = UpdateSecuredSettings(bundlesData);

									var databaseDocuemnt = new DatabaseDocument
									{
										Id = newDatabase.DbName.Text,
										Settings = settings,
										SecuredSettings = securedSettings
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

											HendleBundleAfterCreation(bundlesData, databaseName);

											ExecuteCommand(new ChangeDatabaseCommand(), databaseName);
										})
										.Catch();
								});
						});
				})
				.Catch();
		}

		private Dictionary<string, string> UpdateSecuredSettings(IEnumerable<ChildWindow> bundlesData)
		{
			var settings = new Dictionary<string, string>();


			var encryptionData = bundlesData.FirstOrDefault(window => window is EncryotionSettings) as EncryotionSettings;
			if (encryptionData != null)
			{
				settings[Constants.EncryptionKeySetting] = encryptionData.EncryptionKey.Text;
			}

			return settings;
		}

		private void HendleBundleAfterCreation(List<ChildWindow> bundlesData, string databaseName)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);
			var versioningData = bundlesData.FirstOrDefault(window => window is VersioningSettings) as VersioningSettings;
			if (versioningData != null)
			{
				StoreVersioningData(versioningData, session);
			}

			var replicationData = bundlesData.FirstOrDefault(window => window is ReplicationSettings) as ReplicationSettings;
			if (replicationData != null)
			{
				var replicationDocument = new ReplicationDocument
				{
					Destinations = replicationData.Destinations.ToList()
				};
				session.Store(replicationDocument);
			}

			session.SaveChangesAsync();

			var encryotionSettings = bundlesData.FirstOrDefault(window => window is EncryotionSettings) as EncryotionSettings;
			if (encryotionSettings != null)
				new ShowEncryptionMessage(encryotionSettings.EncryptionKey.Text).Show();

		}

		private void StoreVersioningData(VersioningSettings versioningData, IAsyncDocumentSession session)
		{
			if (versioningData.defaultVersioning.IsChecked == true)
				session.Store(new VersioningConfiguration
				{
					Exclude = false,
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 5
				});

			foreach (var data in versioningData.VersioningData)
			{
				session.Store(data);
			}
		}

		private static Dictionary<string, string> UpdateSettings(NewDatabase newDatabase, BundlesSelect bundles, List<ChildWindow> bundlesData)
		{
			var settings = new Dictionary<string, string>
			{
				{
					Constants.RavenDataDir, newDatabase.ShowAdvanded.IsChecked == true
					                 	? newDatabase.DbPath.Text
					                 	: Path.Combine("~", Path.Combine("Databases", newDatabase.DbName.Text))
					},
				{Constants.ActiveBundles, string.Join(";", bundles.Bundles)}
			};

			if (!string.IsNullOrWhiteSpace(newDatabase.LogsPath.Text))
				settings.Add(Constants.RavenLogsPath, newDatabase.LogsPath.Text);
			if (!string.IsNullOrWhiteSpace(newDatabase.IndexPath.Text))
				settings.Add(Constants.RavenIndexPath, newDatabase.IndexPath.Text);

			var quatasData = bundlesData.FirstOrDefault(window => window is QuotasSettings) as QuotasSettings;
			if (quatasData != null)
			{
				settings[Constants.DocsHardLimit] = (quatasData.MaxDocs.Value).ToString(CultureInfo.InvariantCulture);
				settings[Constants.DocsSoftLimit] = (quatasData.WarnDocs.Value).ToString(CultureInfo.InvariantCulture);
				settings[Constants.SizeHardLimitInKB] = (quatasData.MaxSize.Value * 1024).ToString(CultureInfo.InvariantCulture);
				settings[Constants.SizeSoftLimitInKB] = (quatasData.WarnSize.Value * 1024).ToString(CultureInfo.InvariantCulture);
			}

			return settings;
		}
	}
}