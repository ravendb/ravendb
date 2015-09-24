// -----------------------------------------------------------------------
//  <copyright file="CountersLandlord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Counters;
using Raven.Database.Extensions;
using Raven.Database.Server.Connections;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Server.Security;
using Raven.Json.Linq;

namespace Raven.Database.Server.Tenancy
{
	public class CountersLandlord : AbstractLandlord<CounterStorage>
	{
		private bool initialized;

		public override string ResourcePrefix { get { return Constants.Counter.Prefix; } }

        public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };

		public CountersLandlord(DocumentDatabase systemDatabase) : base(systemDatabase)
		{
			Init();
		}

	    public InMemoryRavenConfiguration SystemConfiguration { get { return systemDatabase.Configuration; } }

		public void Init()
        {
            if (initialized)
                return;
            initialized = true;
            systemDatabase.Notifications.OnDocumentChange += (database, notification, doc) =>
            {
                if (notification.Id == null)
                    return;
				if (notification.Id.StartsWith(ResourcePrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;
				var dbName = notification.Id.Substring(ResourcePrefix.Length);
                Logger.Info("Shutting down counters {0} because the tenant counter document has been updated or removed", dbName);
				Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
            };
        }

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledCounterStorage = false)
		{
			if (string.IsNullOrWhiteSpace(tenantId))
				throw new ArgumentException("tenantId");
			var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledCounterStorage);
			if (document == null)
				return null;

			return CreateConfiguration(tenantId, document, Constants.Counter.DataDirectory, systemDatabase.Configuration);		
        }

        protected InMemoryRavenConfiguration CreateConfiguration(
                        string tenantId,
                        CounterStorageDocument document,
                        string folderPropName,
                        InMemoryRavenConfiguration parentConfiguration)
        {
            var config = new InMemoryRavenConfiguration
            {
                Settings = new NameValueCollection(parentConfiguration.Settings),
            };

            SetupTenantConfiguration(config);

			config.CustomizeValuesForCounterStorageTenant(tenantId);

	        /*config.Settings[Constants.Counter.DataDirectory] = parentConfiguration.Counter.DataDirectory;
            //config.Settings["Raven/StorageEngine"] = parentConfiguration.DefaultStorageTypeName;
			//TODO: what counters storage dir path?
            //config.Settings["Raven/Counters/Storage"] = parentConfiguration.FileSystem.DefaultStorageTypeName;*/

            foreach (var setting in document.Settings)
            {
                config.Settings[setting.Key] = setting.Value;
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.Settings[securedSetting.Key] = securedSetting.Value;
            }

            config.Settings[folderPropName] = config.Settings[folderPropName].ToFullPath(parentConfiguration.Counter.DataDirectory);
            //config.Settings["Raven/Esent/LogsPath"] = config.Settings["Raven/Esent/LogsPath"].ToFullPath(parentConfiguration.DataDirectory);
            config.Settings[Constants.RavenTxJournalPath] = config.Settings[Constants.RavenTxJournalPath].ToFullPath(parentConfiguration.DataDirectory);

            config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

            config.CounterStorageName = tenantId;

            config.Initialize();
            config.CopyParentSettings(parentConfiguration);
            return config;
        }

		private CounterStorageDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledCounterStorage = false)
		{
			JsonDocument jsonDocument;
			using (systemDatabase.DisableAllTriggersForCurrentThread())
				jsonDocument = systemDatabase.Documents.Get(ResourcePrefix + tenantId, null);
			if (jsonDocument == null || jsonDocument.Metadata == null ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
				return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<CounterStorageDocument>();
			if (document.Settings.Keys.Contains(Constants.Counter.DataDirectory) == false)
				throw new InvalidOperationException("Could not find " + Constants.Counter.DataDirectory);

			if (document.Disabled && !ignoreDisabledCounterStorage)
				throw new InvalidOperationException("The counter storage has been disabled.");

			return document;
		}

		public override async Task<CounterStorage> GetResourceInternal(string resourceName)
		{
			Task<CounterStorage> cs;
			if (TryGetOrCreateResourceStore(resourceName, out cs))
				return await cs.ConfigureAwait(false);
			return null;
		}

		public override bool TryGetOrCreateResourceStore(string tenantId, out Task<CounterStorage> counter)
		{
			if (Locks.Contains(DisposingLock))
				throw new ObjectDisposedException("CountersLandlord", "Server is shutting down, can't access any counters");

			if (Locks.Contains(tenantId))
				throw new InvalidOperationException("Counters '" + tenantId + "' is currently locked and cannot be accessed");

			ManualResetEvent cleanupLock;
			if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxSecondsForTaskToWaitForDatabaseToLoad) == false)
				throw new InvalidOperationException(string.Format("Counters '{0}' are currently being restarted and cannot be accessed. We already waited {1} seconds.", tenantId, MaxSecondsForTaskToWaitForDatabaseToLoad));

			if (ResourcesStoresCache.TryGetValue(tenantId, out counter))
			{
				if (counter.IsFaulted || counter.IsCanceled)
				{
					ResourcesStoresCache.TryRemove(tenantId, out counter);
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

			counter = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
			{
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());
				var cs = new CounterStorage(systemDatabase.ServerUrl, tenantId, config, transportState);
				AssertLicenseParameters(config);

				// if we have a very long init process, make sure that we reset the last idle time for this db.
				LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
				return cs;
			}).ContinueWith(task =>
			{
				if (task.Status == TaskStatus.Faulted) // this observes the task exception
				{
					Logger.WarnException("Failed to create counters " + tenantId, task.Exception);
				}
				return task;
			}).Unwrap());
			return true;
		}

        public void Unprotect(CounterStorageDocument configDocument)
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

        public void Protect(CounterStorageDocument configDocument)
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

		private void AssertLicenseParameters(InMemoryRavenConfiguration config)
		{
			string maxDatabases;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfCounters", out maxDatabases))
			{
				if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
				{
					var numberOfAllowedFileSystems = int.Parse(maxDatabases);

					int nextPageStart = 0;
					var databases =
						systemDatabase.Documents.GetDocumentsWithIdStartingWith(ResourcePrefix, null, null, 0,
							numberOfAllowedFileSystems, CancellationToken.None, ref nextPageStart).ToList();
					if (databases.Count >= numberOfAllowedFileSystems)
						throw new InvalidOperationException(
							"You have reached the maximum number of counters that you can have according to your license: " +
							numberOfAllowedFileSystems + Environment.NewLine +
							"You can either upgrade your RavenDB license or delete a counter from the server");
				}
			}

			if (Authentication.IsLicensedForCounters == false)
			{
				throw new InvalidOperationException("Your license does not allow the use of the Counters");
		}

			Authentication.AssertLicensedBundles(config.ActiveBundles);
		}

		public void ForAllCountersInCacheOnly(Action<CounterStorage> action)
		{
			foreach (var value in ResourcesStoresCache
				.Select(cs => cs.Value)
				.Where(value => value.Status == TaskStatus.RanToCompletion))
			{
				action(value.Result);
			}
		}

		public void ForAllCounters(Action<CounterStorage> action)
		{
			using (systemDatabase.DisableAllTriggersForCurrentThread())
			{
				int nextPageStart = 0;
				var counterDocs = systemDatabase.Documents.GetDocumentsWithIdStartingWith(ResourcePrefix, null, null,
					0,int.MaxValue, CancellationToken.None, ref nextPageStart).ToList();

				foreach (var doc in counterDocs)
				{
					var id = GetCounterIdFromDocumentKey(doc);
					Debug.Assert(String.IsNullOrWhiteSpace(id) == false,"key of counter should not be empty");
					Task<CounterStorage> counterFetchTask;
					if (!TryGetOrCreateResourceStore(id, out counterFetchTask))
						throw new InvalidOperationException(string.Format("Could not get counter specified by counter storage document. The id that wasn't found is {0}", id));

					var counter = AsyncHelpers.RunSync(() => counterFetchTask);
					action(counter);
				}
			}
		}

		private static string GetCounterIdFromDocumentKey(RavenJToken doc)
		{
			var metadata = doc.Value<RavenJObject>("@metadata");
			var docKey = metadata.Value<string>("@id");
			var startIndex = docKey.LastIndexOf('/') + 1;
			if (startIndex >= docKey.Length)
				throw new InvalidOperationException(string.Format("Counter document key is invalid. (got {0})", docKey));

			var id = docKey.Substring(startIndex);
			return id;
		}


		protected override DateTime LastWork(CounterStorage resource)
		{
			return resource.LastWrite;
		}
	}
}