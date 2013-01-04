using System;
using System.Collections.ObjectModel;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Extensions;
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

            if (periodicBackup.IsS3Selected.Value)
                periodicBackup.PeriodicBackupSetup.GlacierVaultName = string.Empty;
            else
                periodicBackup.PeriodicBackupSetup.S3BucketName = string.Empty;

            settingsModel.DatabaseDocument.SecuredSettings["Raven/AWSSecretKey"] = periodicBackup.AwsSecretKey;
			settingsModel.DatabaseDocument.Settings["Raven/AWSAccessKey"] = periodicBackup.AwsAccessKey;

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
				ApplicationModel.DatabaseCommands.ForDefaultDatabase().DeleteDocumentAsync(apiKeyDefinition.Id);
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