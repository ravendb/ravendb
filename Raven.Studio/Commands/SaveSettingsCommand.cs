using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Database.Bundles.SqlReplication;
using Raven.Json.Linq;
using Raven.Studio.Controls;
using Raven.Studio.Features.Bundles;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Settings;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class SaveSettingsCommand : Command
	{
        private readonly SettingsModel settingsModel;
		private bool needToSaveChanges;

		public SaveSettingsCommand(SettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		protected override async Task ExecuteAsync(object parameter)
		{
			if(ApplicationModel.Current == null || ApplicationModel.Current.Server.Value == null || ApplicationModel.Current.Server.Value.SelectedDatabase.Value == null)
				return;
			if (settingsModel == null)
				return;

			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);

			if(databaseName == Constants.SystemDatabase)
			{
				var systemSession = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();
				await SaveApiKeys(systemSession);
				var savedWinAuth = await SaveWindowsAuth(systemSession);
				if(needToSaveChanges)
					await systemSession.SaveChangesAsync();
				ApplicationModel.Current.Notifications.Add(savedWinAuth
					? new Notification("Api keys and Windows Authentication saved")
					: new Notification("Only Api keys where saved, something when wrong with Windows Authentication", NotificationLevel.Error));
				return;
			}

			var periodicBackup = settingsModel.GetSection<PeriodicBackupSettingsSectionModel>();
			if (periodicBackup != null)
				await SavePeriodicBackup(periodicBackup, session);

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
				await DatabaseCommands.GlobalAdmin.CreateDatabaseAsync(settingsModel.DatabaseDocument);
			}

		    var replicationSettings = settingsModel.GetSection<ReplicationSettingsSectionModel>();
			if (replicationSettings != null)
			{
				ReplicationDocument document;
				try
				{
					document = await session.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations") ?? new ReplicationDocument();
				}
				catch
				{
					document = new ReplicationDocument();
				}

				document.Destinations.Clear();
				foreach (var destination in replicationSettings.ReplicationDestinations.Where(destination => !string.IsNullOrWhiteSpace(destination.Url)))
				{
					document.Destinations.Add(destination);
				}
				try
				{
					if(document.Destinations.Count != 0 && document.Destinations.Any(destination => destination.Disabled == false))
						await CheckDestinations(document);

					await session.StoreAsync(document);
					needToSaveChanges = true;
				}
				catch (Exception e)
				{
					ApplicationModel.Current.AddErrorNotification(e.GetBaseException());
				}
			
			}

			var scriptedSettings = settingsModel.GetSection<ScriptedIndexSettingsSectionModel>();
			if (scriptedSettings != null)
			{
				scriptedSettings.StoreChanges();
				foreach (var scriptedIndexResults in scriptedSettings.ScriptedIndexes)
				{

					if (scriptedIndexResults.Value != null)
					{
						scriptedIndexResults.Value.Id = ScriptedIndexResults.IdPrefix + scriptedIndexResults.Key;
						await session.StoreAsync(scriptedIndexResults.Value, scriptedIndexResults.Value.Id);
					}
				}

				foreach (var indexName in scriptedSettings.DeletedIndexes)
				{
					var id = ScriptedIndexResults.IdPrefix + indexName;

					await DatabaseCommands.DeleteDocumentAsync(id);
				}

				needToSaveChanges = true;
			}

			var sqlReplicationSettings = settingsModel.GetSection<SqlReplicationSettingsSectionModel>();
			if (sqlReplicationSettings != null)
			{
				if (sqlReplicationSettings.SqlReplicationConfigs.Any(config => string.IsNullOrWhiteSpace(config.Name)) == false)
				{
					var problemWithTable = false;
					foreach (var sqlReplicationConfigModel in sqlReplicationSettings.SqlReplicationConfigs)
					{
						var hashset = new HashSet<string>();
						foreach (var sqlReplicationTable in sqlReplicationConfigModel.SqlReplicationTables)
						{
							var exists = !hashset.Add(sqlReplicationTable.TableName);
							if (string.IsNullOrWhiteSpace(sqlReplicationTable.DocumentKeyColumn) ||
							    string.IsNullOrWhiteSpace(sqlReplicationTable.TableName) || exists)
							{
								problemWithTable = true;
								break;
							}
						}
						if (problemWithTable)
							break;
					}

					if (problemWithTable)
					{
						ApplicationModel.Current.AddNotification(
							new Notification(
								"Sql Replication settings were not saved, all tables must distinct names and have document keys",
								NotificationLevel.Error));
					}
					else
					{
						var hasChanges = new List<string>();
						var documents = await session.Advanced.LoadStartingWithAsync<SqlReplicationConfig>("Raven/SqlReplication/Configuration/", null);

						sqlReplicationSettings.UpdateIds();
						if (documents != null)
						{
							hasChanges = sqlReplicationSettings.SqlReplicationConfigs.Where(config => HasChanges(config,
								documents.FirstOrDefault(replicationConfig =>replicationConfig.Name == config.Name)))
								.Select(config => config.Name).ToList();

							foreach (var sqlReplicationConfig in documents)
							{
								if (sqlReplicationSettings.SqlReplicationConfigs.All(config => config.Id != sqlReplicationConfig.Id))
								{
									session.Delete(sqlReplicationConfig);
								}
							}
						}

						if (hasChanges != null && hasChanges.Count > 0)
						{
							var resetReplication = new ResetReplication(hasChanges);
							await resetReplication.ShowAsync();
							if (resetReplication.Selected.Count == 0)
								return;
							const string ravenSqlreplicationStatus = "Raven/SqlReplication/Status";

							var status = await session.LoadAsync<SqlReplicationStatus>(ravenSqlreplicationStatus);
							if(status == null)
								status = new SqlReplicationStatus();

							foreach (var name in resetReplication.Selected)
							{
								var lastReplicatedEtag = status.LastReplicatedEtags.FirstOrDefault(etag => etag.Name == name);
								if (lastReplicatedEtag != null)
									lastReplicatedEtag.LastDocEtag = Etag.Empty;
							}

							await session.StoreAsync(status,  ravenSqlreplicationStatus);
						}

						foreach (var sqlReplicationConfig in sqlReplicationSettings.SqlReplicationConfigs)
						{
							var id = "Raven/SqlReplication/Configuration/" + sqlReplicationConfig.Name;
							var config = await session.LoadAsync<SqlReplicationConfig>(id);
							config  = UpdateConfig(config, sqlReplicationConfig);
							await session.StoreAsync(config);
						}
					}
					needToSaveChanges = true;
				}
				else
				{
					ApplicationModel.Current.AddNotification(
						new Notification("Sql Replication settings not saved, all replications must have a name", NotificationLevel.Error));
				}
			}

			var versioningSettings = settingsModel.GetSection<VersioningSettingsSectionModel>();
            if (versioningSettings != null)
			{
                var versionsToDelete = versioningSettings.OriginalVersioningConfigurations
					.Where(originalVersioningConfiguration =>
                        versioningSettings.VersioningConfigurations.Contains(originalVersioningConfiguration) == false)
					.ToList();
				foreach (var versioningConfiguration in versionsToDelete)
				{
					await DatabaseCommands.DeleteDocumentAsync(versioningConfiguration.Id);
				}

                foreach (var versioningConfiguration in versioningSettings.VersioningConfigurations)
				{
					if (versioningConfiguration.Id.StartsWith("Raven/Versioning/",StringComparison.OrdinalIgnoreCase) == false)
						versioningConfiguration.Id = "Raven/Versioning/" + versioningConfiguration.Id;
					await session.StoreAsync(versioningConfiguration);
				}

				if (versioningSettings.DatabaseDocument != null)
					await DatabaseCommands.CreateDatabaseAsync(versioningSettings.DatabaseDocument);

				needToSaveChanges = true;
			}

			var authorizationSettings = settingsModel.GetSection<AuthorizationSettingsSectionModel>();
			if (authorizationSettings != null)
			{
				var usersToDelete = authorizationSettings.OriginalAuthorizationUsers
					.Where(authorizationUser => authorizationSettings.AuthorizationUsers.Contains(authorizationUser) == false)
					.ToList();
				foreach (var authorizationUser in usersToDelete)
				{
					await DatabaseCommands.DeleteDocumentAsync(authorizationUser.Id);
				}

				var rolesToDelete = authorizationSettings.OriginalAuthorizationRoles
					.Where(authorizationRole => authorizationSettings.AuthorizationRoles.Contains(authorizationRole) == false)
					.ToList();
				foreach (var authorizationRole in rolesToDelete)
				{
					await DatabaseCommands.DeleteDocumentAsync(authorizationRole.Id);
				}

				foreach (var authorizationRole in authorizationSettings.AuthorizationRoles)
				{
					await session.StoreAsync(authorizationRole);
				}

				foreach (var authorizationUser in authorizationSettings.AuthorizationUsers)
				{
					await session.StoreAsync(authorizationUser);
				}

				needToSaveChanges = true;
			}

			if(needToSaveChanges)
				await session.SaveChangesAsync();
			foreach (var settingsSectionModel in settingsModel.Sections)
			{
				settingsSectionModel.MarkAsSaved();
			}

			ApplicationModel.Current.AddNotification(new Notification("Updated Settings for: " + databaseName));
		}

		private SqlReplicationConfig UpdateConfig(SqlReplicationConfig config, SqlReplicationConfigModel sqlReplicationConfig)
		{
			if (config == null)
			{
				return sqlReplicationConfig.ToSqlReplicationConfig();
			}
			config.ConnectionString = sqlReplicationConfig.ConnectionString;
			config.ConnectionStringName = sqlReplicationConfig.ConnectionStringName;
			config.ConnectionStringSettingName = sqlReplicationConfig.ConnectionStringSettingName;
			config.Disabled = sqlReplicationConfig.Disabled;
			config.FactoryName = sqlReplicationConfig.FactoryName;
			config.Id = sqlReplicationConfig.Id;
			config.Name = sqlReplicationConfig.Name;
			config.RavenEntityName = sqlReplicationConfig.RavenEntityName;
			config.Script = sqlReplicationConfig.Script;
			config.SqlReplicationTables = new List<SqlReplicationTable>(sqlReplicationConfig.SqlReplicationTables);

			return config;
		}

		private async Task CheckDestinations(ReplicationDocument replicationDocument)
		{
			var badReplication = new List<string>();
			var request = ApplicationModel.Current.Server.Value.SelectedDatabase.Value
			                                    .AsyncDatabaseCommands
			                                    .CreateRequest(string.Format("/admin/replicationInfo").NoCache(), "POST");
			await request.WriteAsync(RavenJObject.FromObject(replicationDocument).ToString());
			var responseAsJson = await request.ReadResponseJsonAsync();
			var replicationInfo = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
												   .Deserialize<ReplicationInfoStatus[]>(new RavenJTokenReader(responseAsJson));

			foreach (var replicationInfoStatus in replicationInfo)
			{
				if (replicationInfoStatus.Status != "Valid")
				{
					badReplication.Add(replicationInfoStatus.Url + " - " + replicationInfoStatus.Code);
				}
			}

			if (badReplication.Count != 0)
			{
				var mesage = "Some of the replications could not be reached:" + Environment.NewLine +
				             string.Join(Environment.NewLine, badReplication);

				ApplicationModel.Current.Notifications.Add(new Notification(mesage, NotificationLevel.Warning));
			}
		}

		private bool HasChanges(SqlReplicationConfigModel local, SqlReplicationConfig remote)
		{
			if (remote == null)
				return false;

			if (local.RavenEntityName != remote.RavenEntityName)
				return true;

			if (local.Script != remote.Script)
				return true;

            if (local.Disabled != remote.Disabled)
                return true;

			if (local.ConnectionString != remote.ConnectionString)
				return true;

			if (local.ConnectionStringName != remote.ConnectionStringName)
				return true;

			if (local.ConnectionStringSettingName != remote.ConnectionStringSettingName)
				return true;

			if (local.FactoryName != remote.FactoryName)
				return true;

			return false;
		}

		private async Task SavePeriodicBackup(PeriodicBackupSettingsSectionModel periodicBackup, IAsyncDocumentSession session)
		{
			if(periodicBackup.PeriodicBackupSetup == null)
				return;

			switch (periodicBackup.SelectedOption.Value)
			{
				case 0:
					periodicBackup.PeriodicBackupSetup.GlacierVaultName = null;
					periodicBackup.PeriodicBackupSetup.S3BucketName = null;
                    periodicBackup.PeriodicBackupSetup.AzureStorageContainer = null;
					break;
				case 1:
					periodicBackup.PeriodicBackupSetup.LocalFolderName = null;
					periodicBackup.PeriodicBackupSetup.S3BucketName = null;
                    periodicBackup.PeriodicBackupSetup.AzureStorageContainer = null;
					break;
				case 2:
					periodicBackup.PeriodicBackupSetup.GlacierVaultName = null;
					periodicBackup.PeriodicBackupSetup.LocalFolderName = null;
                    periodicBackup.PeriodicBackupSetup.AzureStorageContainer = null;
					break;
                case 3:
					periodicBackup.PeriodicBackupSetup.GlacierVaultName = null;
			        periodicBackup.PeriodicBackupSetup.S3BucketName = null;
					periodicBackup.PeriodicBackupSetup.LocalFolderName = null;
			        break;
			}

            settingsModel.DatabaseDocument.SecuredSettings["Raven/AWSSecretKey"] = periodicBackup.PeriodicBackupSettings.AwsSecretKey;
			settingsModel.DatabaseDocument.Settings["Raven/AWSAccessKey"] = periodicBackup.PeriodicBackupSettings.AwsAccessKey;
			settingsModel.DatabaseDocument.SecuredSettings["Raven/AzureStorageKey"] = periodicBackup.PeriodicBackupSettings.AzureStorageKey;
			settingsModel.DatabaseDocument.Settings["Raven/AzureStorageAccount"] = periodicBackup.PeriodicBackupSettings.AzureStorageAccount;

			string activeBundles;
			settingsModel.DatabaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);

			if (activeBundles == null || activeBundles.Contains("PeriodicBackup") == false)
			{
				activeBundles = "PeriodicBackup;" + activeBundles;
			}

			settingsModel.DatabaseDocument.Settings["Raven/ActiveBundles"] = activeBundles;

			await DatabaseCommands.GlobalAdmin.CreateDatabaseAsync(settingsModel.DatabaseDocument);

			await session.StoreAsync(periodicBackup.PeriodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);
			needToSaveChanges = true;
		}

		private async Task<bool> SaveWindowsAuth(IAsyncDocumentSession session)
		{
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

			await session.StoreAsync(RavenJObject.FromObject(windowsAuthModel.Document.Value), "Raven/Authorization/WindowsSettings");
			needToSaveChanges = true;
			return true;
		}

		private async Task SaveApiKeys(IAsyncDocumentSession session)
		{
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
				await ApplicationModel.DatabaseCommands.ForSystemDatabase().DeleteDocumentAsync(apiKeyDefinition.Id);
			}

			foreach (var apiKeyDefinition in apiKeysModel.ApiKeys)
			{
				apiKeyDefinition.Id = "Raven/ApiKeys/" + apiKeyDefinition.Name;
				await session.StoreAsync(apiKeyDefinition);
			}

			apiKeysModel.ApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeysModel.ApiKeys);
			needToSaveChanges = true;
		}
	}
}
