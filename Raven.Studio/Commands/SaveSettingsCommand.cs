using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Extensions;
using Raven.Database.Bundles.SqlReplication;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class SaveSettingsCommand : Command
	{
        private readonly SettingsModel settingsModel;

		public SaveSettingsCommand(SettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

			var periodicBackup = settingsModel.GetSection<PeriodicBackupSettingsSectionModel>();
			if (periodicBackup != null)
				SavePeriodicBackup(databaseName, periodicBackup);


			if(databaseName == Constants.SystemDatabase)
			{
				SaveApiKeys();
				if(SaveWindowsAuth())
					ApplicationModel.Current.Notifications.Add(new Notification("Api keys and Windows Authentication saved"));
				else
				{
					ApplicationModel.Current.Notifications.Add(new Notification("Only Api keys where saved, something when wrong with Windows Authentication", NotificationLevel.Error));					
				}
				return;
			}
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);

            var quotaSettings = settingsModel.GetSection<QuotaSettingsSectionModel>();
            if (quotaSettings != null)
			{
				settingsModel.DatabaseDocument.Settings[Constants.SizeHardLimitInKB] =
                    (quotaSettings.MaxSize * 1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.SizeSoftLimitInKB] =
                    (quotaSettings.WarnSize * 1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsHardLimit] =
                    (quotaSettings.MaxDocs).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsSoftLimit] =
                    (quotaSettings.WarnDocs).ToString(CultureInfo.InvariantCulture);
				if (settingsModel.DatabaseDocument.Id == null)
					settingsModel.DatabaseDocument.Id = databaseName;
				DatabaseCommands.CreateDatabaseAsync(settingsModel.DatabaseDocument);
			}

		    var replicationSettings = settingsModel.GetSection<ReplicationSettingsSectionModel>();
            if (replicationSettings != null)
			{
				session.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
					.ContinueOnSuccessInTheUIThread(document =>
					{
                        if (document == null)
                        {
                            document = new ReplicationDocument();
                        }

						document.Destinations.Clear();
                        foreach (var destination in replicationSettings.ReplicationDestinations
							.Where(destination => !string.IsNullOrWhiteSpace(destination.Url)))
						{
							document.Destinations.Add(destination);
						}
						
						session.Store(document);
						session.SaveChangesAsync().Catch();
					})
					.Catch();
			}

			var sqlReplicationSettings = settingsModel.GetSection<SqlReplicationSettingsSectionModel>();
			if (sqlReplicationSettings != null)
			{
				if (sqlReplicationSettings.SqlReplicationConfigs.Any(config => config.Name == "Temp_Name") == false && sqlReplicationSettings.SqlReplicationConfigs.Any(config => string.IsNullOrWhiteSpace(config.Name)) == false)
				{
					var problemWithTable = false;
					foreach (var sqlReplicationConfigModel in sqlReplicationSettings.SqlReplicationConfigs)
					{
						var hashset = new HashSet<string>();
						foreach (var sqlReplicationTable in sqlReplicationConfigModel.SqlReplicationTables)
						{
							var exists = !hashset.Add(sqlReplicationTable.TableName);
							if (string.IsNullOrWhiteSpace(sqlReplicationTable.DocumentKeyColumn) || string.IsNullOrWhiteSpace(sqlReplicationTable.TableName) || exists)
							{
								problemWithTable = true;
								break;
							}
						}
						if(problemWithTable)
							break;
					}

					if (problemWithTable)
					{
						ApplicationModel.Current.AddNotification(new Notification("Sql Replicaiton settings were not saved, all tables must distinct names and have document keys", NotificationLevel.Error));
					}
					else
					{
						session.Advanced.LoadStartingWithAsync<SqlReplicationConfig>("Raven/SqlReplication/Configuration/")
						   .ContinueOnSuccessInTheUIThread(documents =>
						   {
							   sqlReplicationSettings.UpdateIds();
							   if (documents != null)
							   {
								   foreach (var sqlReplicationConfig in documents)
								   {
									   if (sqlReplicationSettings.SqlReplicationConfigs.All(config => config.Id != sqlReplicationConfig.Id))
									   {
										   session.Delete(sqlReplicationConfig);
									   }
								   }
							   }

							   foreach (var sqlReplicationConfig in sqlReplicationSettings.SqlReplicationConfigs)
							   {
								   sqlReplicationConfig.Id = "Raven/SqlReplication/Configuration/" + sqlReplicationConfig.Name;
								   session.Store(sqlReplicationConfig.ToSqlReplicationConfig());
							   }

							   session.SaveChangesAsync().Catch();
						   })
						   .Catch();
					}
					
				}
				else
				{
					ApplicationModel.Current.AddNotification(new Notification("Sql Replicaiton settings not saved, all settings must have a name and it must be different from \"Temp_Name\"", NotificationLevel.Error));
				}
			}

            var versioningSettings = settingsModel.GetSection<VersioningSettingsSectionModel>();
            if (versioningSettings != null)
			{
                var versionsToDelete = versioningSettings.OriginalVersioningConfigurations
					.Where(
						originalVersioningConfiguration =>
                        versioningSettings.VersioningConfigurations.Contains(originalVersioningConfiguration) == false)
					.ToList();
				foreach (var versioningConfiguration in versionsToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(versioningConfiguration.Id);
				}

                foreach (var versioningConfiguration in versioningSettings.VersioningConfigurations)
				{
					if (versioningConfiguration.Id.StartsWith("Raven/Versioning/",StringComparison.InvariantCultureIgnoreCase) == false)
						versioningConfiguration.Id = "Raven/Versioning/" + versioningConfiguration.Id;
					session.Store(versioningConfiguration);
				}
			}

			var authorizationSettings = settingsModel.GetSection<AuthorizationSettingsSectionModel>();
			if (authorizationSettings != null)
			{
				var usersToDelete = authorizationSettings.OriginalAuthorizationUsers
					.Where(authorizationUser => authorizationSettings.AuthorizationUsers.Contains(authorizationUser) == false)
					.ToList();
				foreach (var authorizationUser in usersToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(authorizationUser.Id);
				}

				var rolesToDelete = authorizationSettings.OriginalAuthorizationRoles
					.Where(authorizationRole => authorizationSettings.AuthorizationRoles.Contains(authorizationRole) == false)
					.ToList();
				foreach (var authorizationRole in rolesToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(authorizationRole.Id);
				}

				foreach (var authorizationRole in authorizationSettings.AuthorizationRoles)
				{
					session.Store(authorizationRole);
				}

				foreach (var authorizationUser in authorizationSettings.AuthorizationUsers)
				{
					session.Store(authorizationUser);
				}
			}

			session.SaveChangesAsync()
				.ContinueOnSuccessInTheUIThread(() => ApplicationModel.Current.AddNotification(new Notification("Updated Settings for: " + databaseName)));
		}

		private void SavePeriodicBackup(string databaseName, PeriodicBackupSettingsSectionModel periodicBackup)
		{
			if(periodicBackup.PeriodicBackupSetup == null)
				return;

			switch (periodicBackup.SelectedOption.Value)
			{
				case 0:
					periodicBackup.PeriodicBackupSetup.GlacierVaultName = null;
					periodicBackup.PeriodicBackupSetup.S3BucketName = null;
					break;
				case 1:
					periodicBackup.PeriodicBackupSetup.LocalFolderName = null;
					periodicBackup.PeriodicBackupSetup.S3BucketName = null;
					break;
				case 2:
					periodicBackup.PeriodicBackupSetup.GlacierVaultName = null;
					periodicBackup.PeriodicBackupSetup.LocalFolderName = null;
					break;
			}

            settingsModel.DatabaseDocument.SecuredSettings["Raven/AWSSecretKey"] = periodicBackup.AwsSecretKey;
			settingsModel.DatabaseDocument.Settings["Raven/AWSAccessKey"] = periodicBackup.AwsAccessKey;

			string activeBundles;
			settingsModel.DatabaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);

			if (activeBundles == null || activeBundles.Contains("PeriodicBackup") == false)
			{
				activeBundles = "PeriodicBackup;" + activeBundles;
			}

			settingsModel.DatabaseDocument.Settings["Raven/ActiveBundles"] = activeBundles;

			DatabaseCommands.CreateDatabaseAsync(settingsModel.DatabaseDocument);

			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);
			session.Store(periodicBackup.PeriodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

			session.SaveChangesAsync();
		}

		private bool SaveWindowsAuth()
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();

			var windowsAuthModel = settingsModel.Sections
				.Where(sectionModel => sectionModel is WindowsAuthSettingsSectionModel)
				.Cast<WindowsAuthSettingsSectionModel>()
				.FirstOrDefault();

			if (windowsAuthModel == null)
				return false;

			if (windowsAuthModel.RequiredGroups.Any(data => data.Name == null) ||
			    windowsAuthModel.RequiredGroups.Any(data => data.Name.Contains("\\") == false) || 
				windowsAuthModel.RequiredUsers.Any(data => data.Name == null) ||
			    windowsAuthModel.RequiredUsers.Any(data => data.Name.Contains("\\") == false))
			{
				ApplicationModel.Current.Notifications.Add(
					new Notification("Windows Authentication not saved!. All names must have \"\\\" in them", NotificationLevel.Error));
				return false;
			}


			windowsAuthModel.Document.Value.RequiredGroups = windowsAuthModel.RequiredGroups.ToList();
			windowsAuthModel.Document.Value.RequiredUsers = windowsAuthModel.RequiredUsers.ToList();

			session.Store(RavenJObject.FromObject(windowsAuthModel.Document.Value), "Raven/Authorization/WindowsSettings");

			session.SaveChangesAsync();

			return true;
		}

		private void SaveApiKeys()
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();

			var apiKeysModel = settingsModel.Sections
				.Where(sectionModel => sectionModel is ApiKeysSectionModel)
				.Cast<ApiKeysSectionModel>()
				.FirstOrDefault();

			if (apiKeysModel == null)
				return;

			var apiKeysToDelete = apiKeysModel.OriginalApiKeys
				  .Where(apiKeyDefinition => apiKeysModel.ApiKeys.Contains(apiKeyDefinition) == false)
				  .ToList();

			foreach (var apiKeyDefinition in apiKeysToDelete)
			{
				ApplicationModel.DatabaseCommands.ForSystemDatabase().DeleteDocumentAsync(apiKeyDefinition.Id);
			}

			foreach (var apiKeyDefinition in apiKeysModel.ApiKeys)
			{
				apiKeyDefinition.Id = "Raven/ApiKeys/" + apiKeyDefinition.Name;
				session.Store(apiKeyDefinition);
			}

			session.SaveChangesAsync();
			apiKeysModel.ApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeysModel.ApiKeys);
		}
	}
}