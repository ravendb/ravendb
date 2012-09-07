using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using System.Windows.Controls;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Studio.Controls;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Settings;
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

					if (Path.GetInvalidPathChars().Any(databaseName.Contains))
						throw new ArgumentException("Cannot create a database with invalid path characters: " + databaseName);
					if (ApplicationModel.Current.Server.Value.Databases.Count(s => s == databaseName) != 0)
						throw new ArgumentException("A database with the name " + databaseName + " already exists");

					AssertValidName(databaseName);

							var bundlesModel = new CreateSettingsModel();
							var bundlesSettings = new List<ChildWindow>();
							if (newDatabase.Encryption.IsChecked == true)
								bundlesSettings.Add(new EncryptionSettings());
							if (newDatabase.Quotas.IsChecked == true || newDatabase.Replication.IsChecked == true || newDatabase.Versioning.IsChecked == true)
							{
								bundlesModel = ConfigureSettingsModel(newDatabase);

							    var bundleView = new SettingsDialog()
								{
									DataContext = bundlesModel
								};

								var bundlesSettingsWindow = new ChildWindow()
								{
									Title = "Setup bundles",
									Content = bundleView,
								};

								bundlesSettingsWindow.KeyDown += (sender, args) =>
								{
									if (args.Key == Key.Escape)
										bundlesSettingsWindow.DialogResult = false;
								};

								bundlesSettings.Add(bundlesSettingsWindow);
							}

							new Wizard(bundlesSettings).StartAsync()
								.ContinueOnSuccessInTheUIThread(bundlesData =>
								{
									ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
									var settings = UpdateSettings(newDatabase, newDatabase, bundlesModel);
									var securedSettings = UpdateSecuredSettings(bundlesData);

									var databaseDocuemnt = new DatabaseDocument
									{
										Id = newDatabase.DbName.Text,
										Settings = settings,
										SecuredSettings = securedSettings
									};

									string encryptionKey = null;
									var encryotionSettings = bundlesData.FirstOrDefault(window => window is EncryptionSettings) as EncryptionSettings;
									if (encryotionSettings != null)
										encryptionKey = encryotionSettings.EncryptionKey.Text;

									DatabaseCommands.CreateDatabaseAsync(databaseDocuemnt).ContinueOnSuccess(
										() => DatabaseCommands.ForDatabase(databaseName).EnsureSilverlightStartUpAsync())
										.ContinueOnSuccessInTheUIThread(() =>
										{
											var model = parameter as DatabasesListModel;
											if (model != null)
												model.ForceTimerTicked();
											ApplicationModel.Current.AddNotification(
												new Notification("Database " + databaseName + " created"));

											HendleBundleAfterCreation(bundlesModel, databaseName, encryptionKey);

											ExecuteCommand(new ChangeDatabaseCommand(), databaseName);
										})
										.Catch();
								});
				})
				.Catch();
		}

	    private static CreateSettingsModel ConfigureSettingsModel(NewDatabase newDatabase)
	    {
	        CreateSettingsModel bundlesModel;
	        bundlesModel = new CreateSettingsModel() {Creation = true};

	        if (newDatabase.Quotas.IsChecked == true)
	        {
	            AddSection(bundlesModel, new QuotaSettingsSectionModel()
	            {
	                MaxSize = 50,
	                WarnSize = 45,
	                MaxDocs = 10000,
	                WarnDocs = 8000,
	            });
	        }
	        if (newDatabase.Replication.IsChecked == true)
	        {
	            AddSection(bundlesModel,
	                       new ReplicationSettingsSectionModel() {ReplicationDestinations = {new ReplicationDestination()}});
	        }
	        if (newDatabase.Versioning.IsChecked == true)
	        {
	            AddSection(bundlesModel, new VersioningSettingsSectionModel()
	            {
	                VersioningConfigurations =
	                {
	                    new VersioningConfiguration()
	                    {
	                        Exclude = false,
	                        Id = "Raven/Versioning/DefaultConfiguration",
	                        MaxRevisions = 5
	                    }
	                }
	            });
	        }
	        return bundlesModel;
	    }

	    private static void AddSection(CreateSettingsModel bundlesModel, SettingsSectionModel section)
	    {
	        bundlesModel.Sections.Add(section);
	        if (bundlesModel.SelectedSection.Value == null)
	            bundlesModel.SelectedSection.Value = section;
	    }

	    private Dictionary<string, string> UpdateSecuredSettings(IEnumerable<ChildWindow> bundlesData)
		{
			var settings = new Dictionary<string, string>();


			var encryptionData = bundlesData.FirstOrDefault(window => window is EncryptionSettings) as EncryptionSettings;
			if (encryptionData != null)
			{
				settings[Constants.EncryptionKeySetting] = encryptionData.EncryptionKey.Text;
			}

			return settings;
		}

		private void HendleBundleAfterCreation(CreateSettingsModel settingsModel, string databaseName, string encryptionKey)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);

		    var versioningSection = settingsModel.GetSection<VersioningSettingsSectionModel>();
            if (versioningSection != null)
                StoreVersioningData(versioningSection.VersioningConfigurations, session);

		    var replicationSection = settingsModel.GetSection<ReplicationSettingsSectionModel>();
            if (replicationSection != null)
			{
				var replicationDocument = new ReplicationDocument();
                foreach (var replicationDestination in replicationSection.ReplicationDestinations
					.Where(replicationDestination => !string.IsNullOrWhiteSpace(replicationDestination.Url) || !string.IsNullOrWhiteSpace(replicationDestination.ConnectionStringName)))
				{
					replicationDocument.Destinations.Add(replicationDestination);
				}
				
				session.Store(replicationDocument);
			}

			session.SaveChangesAsync();

			if (!string.IsNullOrEmpty(encryptionKey))
				new ShowEncryptionMessage(encryptionKey).Show();
		}

		private void StoreVersioningData(IEnumerable<VersioningConfiguration> versioningData, IAsyncDocumentSession session)
		{
			foreach (var data in versioningData)
			{
				session.Store(data);
			}
		}

		private static Dictionary<string, string> UpdateSettings(NewDatabase newDatabase, NewDatabase bundles, CreateSettingsModel settingsData)
		{
			var settings = new Dictionary<string, string>
			{
				{
					Constants.RavenDataDir, newDatabase.ShowAdvanced.IsChecked == true
					                 	? newDatabase.DbPath.Text
					                 	: Path.Combine("~", Path.Combine("Databases", newDatabase.DbName.Text))
					},
				{Constants.ActiveBundles, string.Join(";", bundles.Bundles)}
			};

			if (!string.IsNullOrWhiteSpace(newDatabase.LogsPath.Text))
				settings.Add(Constants.RavenLogsPath, newDatabase.LogsPath.Text);
			if (!string.IsNullOrWhiteSpace(newDatabase.IndexPath.Text))
				settings.Add(Constants.RavenIndexPath, newDatabase.IndexPath.Text);

            var quotasData = settingsData.GetSection<QuotaSettingsSectionModel>();

            if (quotasData != null)
			{
                settings[Constants.DocsHardLimit] = (quotasData.MaxDocs).ToString(CultureInfo.InvariantCulture);
                settings[Constants.DocsSoftLimit] = (quotasData.WarnDocs).ToString(CultureInfo.InvariantCulture);
                settings[Constants.SizeHardLimitInKB] = (quotasData.MaxSize * 1024).ToString(CultureInfo.InvariantCulture);
                settings[Constants.SizeSoftLimitInKB] = (quotasData.WarnSize * 1024).ToString(CultureInfo.InvariantCulture);
			}

			return settings;
		}
		
		private static readonly string validDbNameChars = @"([A-Za-z0-9_\-\.]+)";

		public static void AssertValidName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			var result = Regex.Matches(name, validDbNameChars);
			if (result.Count == 0 || result[0].Value != name)
			{
				throw new InvalidOperationException("Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name);
			}
		}
	}
}