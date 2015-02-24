// -----------------------------------------------------------------------
//  <copyright file="FileSystemsLandlord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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
using Raven.Database.FileSystem;
using Raven.Abstractions.FileSystem;
using System.Collections.Specialized;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Connections;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Tenancy
{
    public class FileSystemsLandlord : AbstractLandlord<RavenFileSystem>
    {
		private bool initialized;

        public override string ResourcePrefix { get { return Constants.FileSystem.Prefix; } }

        public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };

        public InMemoryRavenConfiguration SystemConfiguration
        {
            get { return systemDatabase.Configuration; }
        }

	    public FileSystemsLandlord(DocumentDatabase systemDatabase) : base(systemDatabase)
		{
            Init();
		}

        public void Init()
        {
            if (initialized)
                return;

            initialized = true;

            systemDatabase.Notifications.OnDocumentChange += (database, notification, doc) =>
            {
                if (notification.Id == null)
                    return;
                const string ravenDbPrefix = Constants.FileSystem.Prefix;
                if (notification.Id.StartsWith(ravenDbPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;

                var dbName = notification.Id.Substring(ravenDbPrefix.Length);

                Logger.Info("Shutting down filesystem {0} because the tenant file system document has been updated or removed", dbName);

				Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
            };
        }

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledFileSystem = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("tenantId");
            var document = GetTenantFileSystemDocument(tenantId, ignoreDisabledFileSystem);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document, Constants.FileSystem.DataDirectory, this.SystemConfiguration);
        }

        protected InMemoryRavenConfiguration CreateConfiguration(
                                string tenantId,
                                FileSystemDocument document,
                                string folderPropName,
                                InMemoryRavenConfiguration parentConfiguration)
        {
	        var config = new InMemoryRavenConfiguration
	        {
		        Settings = new NameValueCollection(parentConfiguration.Settings),
	        };

	        SetupTenantConfiguration(config);

	        config.CustomizeValuesForFileSystemTenant(tenantId);
            config.Settings[Constants.FileSystem.Storage] = parentConfiguration.FileSystem.DefaultStorageTypeName;

	        foreach (var setting in document.Settings)
	        {
		        config.Settings[setting.Key] = setting.Value;
	        }
	        Unprotect(document);

			foreach (var securedSetting in document.SecuredSettings)
			{
				config.Settings[securedSetting.Key] = securedSetting.Value;
			}

	        config.Settings[folderPropName] = config.Settings[folderPropName].ToFullPath(parentConfiguration.FileSystem.DataDirectory);
	        config.FileSystemName = tenantId;

	        config.Initialize();
	        config.CopyParentSettings(parentConfiguration);
	        return config;
        }

	    public void Unprotect(FileSystemDocument configDocument)
        {
            if (configDocument.SecuredSettings == null)
            {
                configDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in configDocument.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Convert.FromBase64String(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                try
                {
                    var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    configDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
                }
                catch (Exception e)
                {
                    Logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    configDocument.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }

        public void Protect(FileSystemDocument configDocument)
        {
            if (configDocument.SecuredSettings == null)
            {
                configDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in configDocument.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Encoding.UTF8.GetBytes(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
                configDocument.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
            }
        }

        private FileSystemDocument GetTenantFileSystemDocument(string tenantId, bool ignoreDisabledFileSystem = false)
        {
            JsonDocument jsonDocument;
            using (systemDatabase.DisableAllTriggersForCurrentThread())
            {
                jsonDocument = systemDatabase.Documents.Get(Constants.FileSystem.Prefix + tenantId, null);
            }

            if (jsonDocument == null || jsonDocument.Metadata == null ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
                return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<FileSystemDocument>();
            if (document.Settings.Keys.Contains(Constants.FileSystem.DataDirectory) == false)
                throw new InvalidOperationException("Could not find Raven/FileSystem/DataDir");

			if (document.Disabled && !ignoreDisabledFileSystem)
                throw new InvalidOperationException("The file system has been disabled.");

            return document;
        }

        public bool TryGetFileSystem(string tenantId, out Task<RavenFileSystem> fileSystem)
        {
            return ResourcesStoresCache.TryGetValue(tenantId, out fileSystem);
        }

        public bool TryGetOrCreateResourceStore(string tenantId, out Task<RavenFileSystem> fileSystem)
        {
			if (Locks.Contains(DisposingLock))
				throw new ObjectDisposedException("FileSystem", "Server is shutting down, can't access any file systems");

			if (Locks.Contains(tenantId))
				throw new InvalidOperationException("FileSystem '" + tenantId + "' is currently locked and cannot be accessed");

			ManualResetEvent cleanupLock;
			if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxSecondsForTaskToWaitForDatabaseToLoad) == false)
				throw new InvalidOperationException(string.Format("File system '{0}' is currently being restarted and cannot be accessed. We already waited {1} seconds.", tenantId, MaxSecondsForTaskToWaitForDatabaseToLoad));

            if (ResourcesStoresCache.TryGetValue(tenantId, out fileSystem))
            {
                if (fileSystem.IsFaulted || fileSystem.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(tenantId, out fileSystem);
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

            fileSystem = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
            {
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());

				AssertLicenseParameters(config);
				var fs = new RavenFileSystem(config, tenantId, transportState);
                fs.Initialize();

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return fs;
            }).ContinueWith(task =>
            {
                if (task.Status == TaskStatus.Faulted) // this observes the task exception
                {
                    Logger.WarnException("Failed to create filesystem " + tenantId, task.Exception);
                }
                return task;
            }).Unwrap());
            return true;
        }

        private void AssertLicenseParameters(InMemoryRavenConfiguration config)
        {
			string maxFileSystmes;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfFileSystems", out maxFileSystmes))
            {
                if (string.Equals(maxFileSystmes, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
                {
					var numberOfAllowedFileSystems = int.Parse(maxFileSystmes);

                    int nextPageStart = 0;
                    var fileSystems =
                        systemDatabase.Documents.GetDocumentsWithIdStartingWith(Constants.FileSystem.Prefix, null, null, 0,
                            numberOfAllowedFileSystems, CancellationToken.None, ref nextPageStart).ToList();
                    if (fileSystems.Count >= numberOfAllowedFileSystems)
                        throw new InvalidOperationException(
                            "You have reached the maximum number of file systems that you can have according to your license: " +
                            numberOfAllowedFileSystems + Environment.NewLine +
                            "You can either upgrade your RavenDB license or delete a file system from the server");
                }
            }

	        if (Authentication.IsLicensedForRavenFs == false)
	        {
				throw new InvalidOperationException("Your license does not allow the use of the RavenFS");
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

	    protected override DateTime LastWork(RavenFileSystem resource)
        {
            return resource.SynchronizationTask.LastSuccessfulSynchronizationTime;
        }

        public async Task<RavenFileSystem> GetFileSystemInternal(string name)
        {
            Task<RavenFileSystem> db;
            if (TryGetOrCreateResourceStore(name, out db))
                return await db;
            return null;
        }

        public void ForAllFileSystems(Action<RavenFileSystem> action)
        {
            foreach (var value in ResourcesStoresCache
                .Select(db => db.Value)
                .Where(value => value.Status == TaskStatus.RanToCompletion))
            {
                action(value.Result);
            }
        }

	    public bool IsFileSystemLoaded(string tenantName)
	    {
			Task<RavenFileSystem> dbTask;
			if (ResourcesStoresCache.TryGetValue(tenantName, out dbTask) == false)
				return false;

			return dbTask != null && dbTask.Status == TaskStatus.RanToCompletion;
	    }
    }
}