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
using Raven.Database.Server.RavenFS;

namespace Raven.Database.Server.Tenancy
{
    public class FileSystemsLandlord : AbstractLandlord<RavenFileSystem>
    {
		private bool initialized;
        private readonly DocumentDatabase systemDatabase;
        private const string FILESYSTEMS_PREFIX = "Raven/FileSystems/";
        public override string ResourcePrefix { get { return FILESYSTEMS_PREFIX; } }

        public InMemoryRavenConfiguration SystemConfiguration
        {
            get { return systemDatabase.Configuration; }
        }

        public FileSystemsLandlord(DocumentDatabase systemDatabase)
		{
			this.systemDatabase = systemDatabase;

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
                const string ravenDbPrefix = "Raven/FileSystems/";
                if (notification.Id.StartsWith(ravenDbPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;
                var dbName = notification.Id.Substring(ravenDbPrefix.Length);
                Logger.Info("Shutting down filesystem {0} because the tenant fs document has been updated or removed", dbName);
				Cleanup(dbName, skipIfActive: false, notificationType: notification.Type);
            };
        }

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledFileSystem = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("tenantId");
			var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledFileSystem);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document,"Raven/FileSystem/DataDir", systemDatabase.Configuration);
        }

		private DatabaseDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledFileSystem = false)
        {
            JsonDocument jsonDocument;
            using (systemDatabase.DisableAllTriggersForCurrentThread())
                jsonDocument = systemDatabase.Documents.Get("Raven/FileSystems/" + tenantId, null);
            if (jsonDocument == null ||
                jsonDocument.Metadata == null ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
                return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (document.Settings.Keys.Contains("Raven/FileSystem/DataDir") == false)
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

            if (Locks.Contains(tenantId))
                throw new InvalidOperationException("FileSystem '" + tenantId + "' is currently locked and cannot be accessed");

            var config = CreateTenantConfiguration(tenantId);
            if (config == null)
                return false;

            fileSystem = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
            {
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());

				AssertLicenseParameters();
				var fs = new RavenFileSystem(config, tenantId, transportState);

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

        private void AssertLicenseParameters()
        {
			string maxFileSystmes;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfFileSystems", out maxFileSystmes))
            {
                if (string.Equals(maxFileSystmes, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
                {
					var numberOfAllowedFileSystems = int.Parse(maxFileSystmes);

                    int nextPageStart = 0;
                    var fileSystems =
                        systemDatabase.Documents.GetDocumentsWithIdStartingWith("Raven/FileSystems/", null, null, 0,
                            numberOfAllowedFileSystems, CancellationToken.None, ref nextPageStart).ToList();
                    if (fileSystems.Count >= numberOfAllowedFileSystems)
                        throw new InvalidOperationException(
                            "You have reached the maximum number of file systems that you can have according to your license: " +
                            numberOfAllowedFileSystems + Environment.NewLine +
                            "You can either upgrade your RavenDB license or delete a file system from the server");
                }
            }

	        if (IsNotLicensed())
	        {
				throw new InvalidOperationException("Your license does not allow the use of the RavenFS");
	        }
        }

		public static bool IsNotLicensed()
	    {
			//string ravenFsValue;
			//var license = ValidateLicense.CurrentLicense;
			//if (license.IsCommercial == false)
			//{
			//	return false; // we allow the use of ravenfs in the OSS version
			//}
			//if (license.Attributes.TryGetValue("ravenfs", out ravenFsValue))
			//{
			//	bool active;
			//	if (bool.TryParse(ravenFsValue, out active))
			//		return active == false;
			//}
			//return true;
			return false;
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
    }
}