using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Connections;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Server.Tenancy
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {

        public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };

	    private bool initialized;
        private const string DATABASES_PREFIX = "Raven/Databases/";
        public override string ResourcePrefix { get { return DATABASES_PREFIX; } }

        public DatabasesLandlord(DocumentDatabase systemDatabase) : base(systemDatabase)
        {
			string tempPath = Path.GetTempPath();
			var fullTempPath = tempPath + Constants.TempUploadsDirectoryName;
			if (File.Exists(fullTempPath))
				File.Delete(fullTempPath);
			if (Directory.Exists(fullTempPath))
				Directory.Delete(fullTempPath, true);

            Init();
        }

        public DocumentDatabase SystemDatabase
        {
            get { return systemDatabase; }
        }

        public InMemoryRavenConfiguration SystemConfiguration
        {
            get { return systemConfiguration; }
        }

        public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledDatabase = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || tenantId.Equals("<system>", StringComparison.OrdinalIgnoreCase))
                return systemConfiguration;
			var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledDatabase);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document, "Raven/DataDir", systemConfiguration);
        }

		private DatabaseDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledDatabase = false)
        {
            JsonDocument jsonDocument;
            using (systemDatabase.DisableAllTriggersForCurrentThread())
                jsonDocument = systemDatabase.Documents.Get("Raven/Databases/" + tenantId, null);
            if (jsonDocument == null ||
                jsonDocument.Metadata == null ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
                return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (document.Settings["Raven/DataDir"] == null)
                throw new InvalidOperationException("Could not find Raven/DataDir");

			if (document.Disabled && !ignoreDisabledDatabase)
                throw new InvalidOperationException("The database has been disabled.");

            return document;
        }

        public async Task<DocumentDatabase> GetDatabaseInternal(string name)
        {
            if (string.Equals("<system>", name, StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(name))
                return systemDatabase;

            Task<DocumentDatabase> db;
            if (TryGetOrCreateResourceStore(name, out db))
                return await db;
            return null;
        }

        public bool TryGetOrCreateResourceStore(string tenantId, out Task<DocumentDatabase> database)
        {
			if (Locks.Contains(DisposingLock))
				throw new ObjectDisposedException("DatabaseLandlord","Server is shutting down, can't access any databases");

			if (Locks.Contains(tenantId))
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed.");

	        ManualResetEvent cleanupLock;
			if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxSecondsForTaskToWaitForDatabaseToLoad) == false)
				throw new InvalidOperationException(string.Format("Database '{0}' is currently being restarted and cannot be accessed. We already waited {1} seconds.", tenantId, MaxSecondsForTaskToWaitForDatabaseToLoad));

	        if (ResourcesStoresCache.TryGetValue(tenantId, out database))
	        {
		        if (database.IsFaulted || database.IsCanceled)
		        {
			        ResourcesStoresCache.TryRemove(tenantId, out database);
			        DateTime time;
			        LastRecentlyUsed.TryRemove(tenantId, out time);
			        // and now we will try creating it again
		        }
		        else
		        {
			        return true;
		        }
	        }

	        var config = CreateTenantConfiguration(tenantId);
            if (config == null)
                return false;

            database = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
            {
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());

                AssertLicenseParameters(config);
                var documentDatabase = new DocumentDatabase(config, systemDatabase, transportState);

				documentDatabase.SpinBackgroundWorkers();
				documentDatabase.Disposing += DocumentDatabaseDisposingStarted;
				documentDatabase.DisposingEnded += DocumentDatabaseDisposingEnded;
	            documentDatabase.StorageInaccessible += UnloadDatabaseOnStorageInaccessible;
                // register only DB that has incremental backup set.
                documentDatabase.OnBackupComplete += OnDatabaseBackupCompleted;

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return documentDatabase;
            }).ContinueWith(task =>
            {
                if (task.Status == TaskStatus.Faulted) // this observes the task exception
                {
                    Logger.WarnException("Failed to create database " + tenantId, task.Exception);
                }
                return task;
            }).Unwrap());

			if (database.IsFaulted && database.Exception != null)
			{
				// if we are here, there is an error, and if there is an error, we need to clear it from the 
				// resource store cache so we can try to reload it.
				// Note that we return the faulted task anyway, because we need the user to look at the error
				if (database.Exception.Data.Contains("Raven/KeepInResourceStore") == false)
				{
					Task<DocumentDatabase> val;
					ResourcesStoresCache.TryRemove(tenantId, out val);
				}
			}

            return true;
        }

        protected InMemoryRavenConfiguration CreateConfiguration(
                        string tenantId,
                        DatabaseDocument document,
                        string folderPropName,
                        InMemoryRavenConfiguration parentConfiguration)
        {
            var config = new InMemoryRavenConfiguration
            {
                Settings = new NameValueCollection(parentConfiguration.Settings),
            };

            SetupTenantConfiguration(config);

            config.CustomizeValuesForDatabaseTenant(tenantId);

            config.Settings["Raven/StorageEngine"] = parentConfiguration.DefaultStorageTypeName;           

            foreach (var setting in document.Settings)
            {
                config.Settings[setting.Key] = setting.Value;
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.Settings[securedSetting.Key] = securedSetting.Value;
            }

			config.Settings[folderPropName] = config.Settings[folderPropName].ToFullPath(parentConfiguration.DataDirectory);

            config.Settings["Raven/Esent/LogsPath"] = config.Settings["Raven/Esent/LogsPath"].ToFullPath(parentConfiguration.DataDirectory);
            config.Settings[Constants.RavenTxJournalPath] = config.Settings[Constants.RavenTxJournalPath].ToFullPath(parentConfiguration.DataDirectory);

            config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

            config.DatabaseName = tenantId;
            config.IsTenantDatabase = true;

            config.Initialize();
            config.CopyParentSettings(parentConfiguration);
            return config;
        }

        public void Unprotect(DatabaseDocument databaseDocument)
        {
            if (databaseDocument.SecuredSettings == null)
            {
                databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in databaseDocument.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Convert.FromBase64String(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                try
                {
                    var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
                }
                catch (Exception e)
                {
                    Logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }

        public void Protect(DatabaseDocument databaseDocument)
        {
            if (databaseDocument.SecuredSettings == null)
            {
                databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in databaseDocument.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Encoding.UTF8.GetBytes(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
                databaseDocument.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
            }
        }

        private void OnDatabaseBackupCompleted(DocumentDatabase db)
        {
            var dbStatusKey = "Raven/BackupStatus/" + db.Name;
            var statusDocument = db.Documents.Get(dbStatusKey, null);
            DatabaseOperationsStatus status;
            if (statusDocument == null)
            {
                status = new DatabaseOperationsStatus();
            }
            else
            {
                status = statusDocument.DataAsJson.JsonDeserialization<DatabaseOperationsStatus>();
            }
            status.LastBackup = SystemTime.UtcNow;
            var json = RavenJObject.FromObject(status);
            json.Remove("Id");
            systemDatabase.Documents.Put(dbStatusKey, null, json, new RavenJObject(), null);
        }

        private void AssertLicenseParameters(InMemoryRavenConfiguration config)
        {
            string maxDatabases;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfDatabases", out maxDatabases))
            {
                if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
                {
                    var numberOfAllowedDbs = int.Parse(maxDatabases);

                    int nextPageStart = 0;
                    var databases = systemDatabase.Documents.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, numberOfAllowedDbs, CancellationToken.None, ref nextPageStart).ToList();
                    if (databases.Count >= numberOfAllowedDbs)
                        throw new InvalidOperationException(
                            "You have reached the maximum number of databases that you can have according to your license: " + numberOfAllowedDbs + Environment.NewLine +
                            "You can either upgrade your RavenDB license or delete a database from the server");
                }
            }

            foreach (var bundle in config.ActiveBundles.Where(bundle => bundle != "PeriodicExport"))
            {
                string value;
                if (ValidateLicense.CurrentLicense.Attributes.TryGetValue(bundle, out value))
                {
                    bool active;
                    if (bool.TryParse(value, out active) && active == false)
                        throw new InvalidOperationException("Your license does not allow the use of the " + bundle + " bundle.");
                }
            }
        }

        public void ForAllDatabases(Action<DocumentDatabase> action, bool excludeSystemDatabase = false)
        {
            if (!excludeSystemDatabase) action(systemDatabase);
            foreach (var value in ResourcesStoresCache
                .Select(db => db.Value)
                .Where(value => value.Status == TaskStatus.RanToCompletion))
            {
                action(value.Result);
            }
        }

        protected override DateTime LastWork(DocumentDatabase resource)
        {
            return resource.WorkContext.LastWorkTime;
        }

        public void Init()
        {
            if (initialized)
                return;
            initialized = true;
            SystemDatabase.Notifications.OnDocumentChange += (database, notification, doc) =>
            {
                if (notification.Id == null)
                    return;
                const string ravenDbPrefix = "Raven/Databases/";
                if (notification.Id.StartsWith(ravenDbPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;
                var dbName = notification.Id.Substring(ravenDbPrefix.Length);
                Logger.Info("Shutting down database {0} because the tenant database has been updated or removed", dbName);
				Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
            };
        }

        public bool IsDatabaseLoaded(string tenantName)
        {
            if (tenantName == Constants.SystemDatabase)
                return true;

            Task<DocumentDatabase> dbTask;
            if (ResourcesStoresCache.TryGetValue(tenantName, out dbTask) == false)
                return false;

            return dbTask != null && dbTask.Status == TaskStatus.RanToCompletion;
        }

		private void DocumentDatabaseDisposingStarted(object documentDatabase, EventArgs args)
		{
			try
			{
				var database = documentDatabase as DocumentDatabase;
				if (database == null)
				{
					return;
				}

				ResourcesStoresCache.Set(database.Name, (dbName) =>
				{
					var tcs = new TaskCompletionSource<DocumentDatabase>();
					tcs.SetException(new ObjectDisposedException(dbName, "Database named " + dbName + " is being disposed right now and cannot be accessed.\r\n" +
																 "Access will be available when the dispose process will end")
					{
						Data =
						{
							{"Raven/KeepInResourceStore", "true"}
						}
					});
					return tcs.Task;
				});
			}
			catch (Exception ex)
			{
				Logger.WarnException("Failed to substitute database task with temporary place holder. This should not happen", ex);
			}
		}

		private void DocumentDatabaseDisposingEnded(object documentDatabase, EventArgs args)
		{
			try
			{
				var database = documentDatabase as DocumentDatabase;
				if (database == null)
				{
					return;
				}

				ResourcesStoresCache.Remove(database.Name);
			}
			catch (Exception ex)
			{
				Logger.ErrorException("Failed to remove database at the end of the disposal. This should not happen", ex);
			}
		}

		private void UnloadDatabaseOnStorageInaccessible(object documentDatabase, EventArgs eventArgs)
		{
			try
			{
				var database = documentDatabase as DocumentDatabase;
				if (database == null)
				{
					return;
				}

				Task.Run(() =>
				{
					Thread.Sleep(2000); // let the exception thrown by the storage to be propagated into the client

					Logger.Warn("Shutting down database {0} because its storage has become inaccessible", database.Name);

					Cleanup(database.Name, skipIfActiveInDuration: null, shouldSkip: x => false);
				});

			}
			catch (Exception ex)
			{
				Logger.ErrorException("Failed to cleanup database that storage is inaccessible. This should not happen", ex);
			}

		}
    }
}