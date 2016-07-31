using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Json;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {
        public event Action<string> OnDatabaseLoaded = delegate { };
        private readonly HttpJsonRequestFactory _httpJsonRequestFactory = new HttpJsonRequestFactory(64);
        private readonly DocumentConvention _convention = new DocumentConvention();

        public override Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName)
        {
            if (Locks.Contains(DisposingLock))
                throw new ObjectDisposedException("DatabaseLandlord", "Server is shutting down, can't access any databases");
            
            if (Locks.Contains(databaseName))
                throw new InvalidOperationException($"Database '{databaseName}' is currently locked and cannot be accessed.");

            Task<DocumentDatabase> database;
            if (ResourcesStoresCache.TryGetValue(databaseName, out database))
            {
                if (database.IsFaulted || database.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(databaseName, out database);
                    DateTime time;
                    LastRecentlyUsed.TryRemove(databaseName, out time);
                    // and now we will try creating it again
                }
                else
                {
                    return database;
                }
            }

            var config = CreateDatabaseConfiguration(databaseName);
            if (config == null)
                return null;

            var hasAcquired = false;
            try
            {
                if (!ResourceSemaphore.Wait(ConcurrentResourceLoadTimeout))
                    throw new ConcurrentLoadTimeoutException("Too much databases loading concurrently, timed out waiting for them to load.");

                hasAcquired = true;

                var task = new Task<DocumentDatabase>(() => CreateDocumentsStorage(databaseName, config));
                database = ResourcesStoresCache.GetOrAdd(databaseName, task);
                if (database == task)
                    task.Start();

                if (database.IsFaulted && database.Exception != null)
                {
                    // if we are here, there is an error, and if there is an error, we need to clear it from the 
                    // resource store cache so we can try to reload it.
                    // Note that we return the faulted task anyway, because we need the user to look at the error
                    if (database.Exception.Data.Contains("Raven/KeepInResourceStore") == false)
                    {
                        Task<DocumentDatabase> val;
                        ResourcesStoresCache.TryRemove(databaseName, out val);
                    }
                }

                return database;
            }
            finally
            {
                if (hasAcquired)
                    ResourceSemaphore.Release();
            }
        }

        private DocumentDatabase CreateDocumentsStorage(StringSegment databaseName, RavenConfiguration config)
        {
            try
            {
                var sp = Stopwatch.StartNew();
                var documentDatabase = new DocumentDatabase(config.DatabaseName, config, ServerStore.IoMetrics);
                documentDatabase.Initialize();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Started database {config.DatabaseName} in {sp.ElapsedMilliseconds:#,#;;0}ms");

                OnDatabaseLoaded(config.DatabaseName);

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(databaseName, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return documentDatabase;
            }
            catch(Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to start database {config.DatabaseName}", e);
                throw;
            }
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            if (databaseName.IsNullOrWhiteSpace())
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be empty");
            if (databaseName.Equals("<system>")) // This is here to guard against old ravendb tests
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be <system>. Using of <system> database indicates outdated code that was targeted RavenDB 3.5.");

            var document = GetDatabaseDocument(databaseName, ignoreDisabledDatabase);
            if (document == null)
                return null;

            return CreateConfiguration(databaseName, document, RavenConfiguration.GetKey(x => x.Core.DataDirectory));
        }

        protected RavenConfiguration CreateConfiguration(StringSegment databaseName, DatabaseDocument document, string folderPropName)
        {
            var config = RavenConfiguration.CreateFrom(ServerStore.Configuration);

            foreach (var setting in document.Settings)
            {
                config.SetSetting(setting.Key, setting.Value);
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.SetSetting(securedSetting.Key, securedSetting.Value);
            }

            config.SetSetting(folderPropName, config.GetSetting(folderPropName).ToFullPath(ServerStore.Configuration.Core.DataDirectory));
            config.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), config.GetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath)).ToFullPath(ServerStore.Configuration.Core.DataDirectory));

            config.DatabaseName = databaseName.ToString();

            config.Initialize();
            config.CopyParentSettings(ServerStore.Configuration);
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
                    /*var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);*/
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }

        private DatabaseDocument GetDatabaseDocument(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            // We allocate the context here because it should be relatively rare operation
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbId = Constants.Database.Prefix + databaseName;
                var jsonReaderObject = ServerStore.Read(context, dbId);
                if (jsonReaderObject == null)
                    return null;

                var document = JsonDeserialization.DatabaseDocument(jsonReaderObject);

                var dataDirectoryKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
                string dataDirectory;
                if (document.Settings.TryGetValue(dataDirectoryKey,out dataDirectory) == false || dataDirectory == null)
                    throw new InvalidOperationException($"Could not find {dataDirectoryKey}");

                if (document.Disabled && !ignoreDisabledDatabase)
                    throw new InvalidOperationException("The database has been disabled.");

                return document;
            }
        }

        public DatabasesLandlord(ServerStore serverStore) : base(serverStore)
        {

        }
    }
}