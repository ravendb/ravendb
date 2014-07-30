using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Server.Connections;

namespace Raven.Database.Server.Tenancy
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {
        private readonly InMemoryRavenConfiguration systemConfiguration;
        private readonly DocumentDatabase systemDatabase;

	    private bool initialized;
        public DatabasesLandlord(DocumentDatabase systemDatabase)
        {
            systemConfiguration = systemDatabase.Configuration;
            this.systemDatabase = systemDatabase;

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

            if (Locks.Contains(tenantId))
                throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");

            var config = CreateTenantConfiguration(tenantId);
            if (config == null)
                return false;

            database = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
            {
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());
				var documentDatabase = new DocumentDatabase(config, transportState);
                AssertLicenseParameters(config);
                documentDatabase.SpinBackgroundWorkers();

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
            return true;
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

        public void ForAllDatabases(Action<DocumentDatabase> action)
        {
            action(systemDatabase);
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
				Cleanup(dbName, skipIfActive: false, notificationType: notification.Type);
            };
        }
    }
}