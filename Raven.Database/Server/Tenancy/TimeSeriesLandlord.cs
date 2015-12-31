// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesLandlord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.TimeSeries;
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
using Raven.Abstractions.Exceptions;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Tenancy
{
    public class TimeSeriesLandlord : AbstractLandlord<TimeSeriesStorage>
    {
        private bool initialized;

        public override string ResourcePrefix { get { return Constants.TimeSeries.Prefix; } }

        public event Action<RavenConfiguration> SetupTenantConfiguration = delegate { };

        public TimeSeriesLandlord(DocumentDatabase systemDatabase)
            : base(systemDatabase)
        {
            Init();
        }

        public RavenConfiguration SystemConfiguration { get { return systemDatabase.Configuration; } }

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
                Logger.Info("Shutting down time series {0} because the tenant time series document has been updated or removed", dbName);
                Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
            };
        }

        public RavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledTimeSeries = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("tenantId");
            var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledTimeSeries);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document, RavenConfiguration.GetKey(x => x.TimeSeries.DataDirectory), systemDatabase.Configuration);
        }

        protected RavenConfiguration CreateConfiguration(
                        string tenantId,
                        TimeSeriesDocument document,
                        string folderPropName,
                        RavenConfiguration parentConfiguration)
        {
            var config = RavenConfiguration.CreateFrom(parentConfiguration);

            SetupTenantConfiguration(config);

            config.CustomizeValuesForTimeSeriesTenant(tenantId);

            config.SetSetting(RavenConfiguration.GetKey(x => x.TimeSeries.DataDirectory), parentConfiguration.TimeSeries.DataDirectory);

            foreach (var setting in document.Settings)
            {
                config.SetSetting(setting.Key, setting.Value);
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.SetSetting(securedSetting.Key, securedSetting.Value);
            }

            config.SetSetting(folderPropName, config.GetSetting(folderPropName).ToFullPath(parentConfiguration.Core.DataDirectory));
            config.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), config.GetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath).ToFullPath(parentConfiguration.Core.DataDirectory)));

            config.TimeSeriesName = tenantId;

            config.Initialize();
            config.CopyParentSettings(parentConfiguration);
            return config;
        }

        private TimeSeriesDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledTimeSeriesStorage = false)
        {
            JsonDocument jsonDocument;
            using (systemDatabase.DisableAllTriggersForCurrentThread())
                jsonDocument = systemDatabase.Documents.Get(ResourcePrefix + tenantId);
            if (jsonDocument == null || jsonDocument.Metadata == null ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
                return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<TimeSeriesDocument>();
            if (document.Settings.Keys.Contains(RavenConfiguration.GetKey(x => x.TimeSeries.DataDirectory)) == false)
                throw new InvalidOperationException("Could not find " + RavenConfiguration.GetKey(x => x.TimeSeries.DataDirectory));

            if (document.Disabled && !ignoreDisabledTimeSeriesStorage)
                throw new InvalidOperationException("The time series has been disabled.");

            return document;
        }

        public override async Task<TimeSeriesStorage> GetResourceInternal(string resourceName)
        {
            Task<TimeSeriesStorage> cs;
            if (TryGetOrCreateResourceStore(resourceName, out cs))
                return await cs.ConfigureAwait(false);
            return null;
        }

        public override bool TryGetOrCreateResourceStore(string tenantId, out Task<TimeSeriesStorage> timeSeries)
        {
            if (Locks.Contains(DisposingLock))
                throw new ObjectDisposedException("TimeSeriesLandlord", "Server is shutting down, can't access any time series");

            if (Locks.Contains(tenantId))
                throw new InvalidOperationException("TimeSeries '" + tenantId + "' is currently locked and cannot be accessed");

            ManualResetEvent cleanupLock;
            if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxTimeForTaskToWaitForDatabaseToLoad) == false)
                throw new InvalidOperationException($"TimeSeries '{tenantId}' are currently being restarted and cannot be accessed. We already waited {MaxTimeForTaskToWaitForDatabaseToLoad.TotalSeconds} seconds.");

            if (ResourcesStoresCache.TryGetValue(tenantId, out timeSeries))
            {
                if (timeSeries.IsFaulted || timeSeries.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(tenantId, out timeSeries);
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

            var hasAcquired = false;
            try
            {
                if (!ResourceSemaphore.Wait(ConcurrentResourceLoadTimeout))
                    throw new ConcurrentLoadTimeoutException("Too much counters loading concurrently, timed out waiting for them to load.");

                hasAcquired = true;
            timeSeries = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
            {
                var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());
                var cs = new TimeSeriesStorage(systemDatabase.ServerUrl, tenantId, config, transportState);
                AssertLicenseParameters(config);

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return cs;
            }).ContinueWith(task =>
            {
                if (task.Status == TaskStatus.Faulted) // this observes the task exception
                {
                    Logger.WarnException("Failed to create time series " + tenantId, task.Exception);
                }
                return task;
            }).Unwrap());
            return true;
            }
            finally
            {
                if (hasAcquired)
                    ResourceSemaphore.Release();
            }
        }

        public void Unprotect(TimeSeriesDocument configDocument)
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

        public void Protect(TimeSeriesDocument configDocument)
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

        private void AssertLicenseParameters(RavenConfiguration config)
        {
            string maxTimeSeries;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfTimeSeries", out maxTimeSeries))
            {
                if (string.Equals(maxTimeSeries, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
                {
                    var numberOfAllowedTimeSeries = int.Parse(maxTimeSeries);

                    int nextPageStart = 0;
                    var timeSeries =
                        systemDatabase.Documents.GetDocumentsWithIdStartingWith(ResourcePrefix, null, null, 0,
                            numberOfAllowedTimeSeries, CancellationToken.None, ref nextPageStart).ToList();
                    if (timeSeries.Count >= numberOfAllowedTimeSeries)
                        throw new InvalidOperationException(
                            "You have reached the maximum number of time series that you can have according to your license: " +
                            numberOfAllowedTimeSeries + Environment.NewLine +
                            "But we detect: " + timeSeries.Count + " time series" + Environment.NewLine +
                            "You can either upgrade your RavenDB license or delete a time series from the server");
                }
            }

            if (Authentication.IsLicensedForTimeSeries == false)
            {
                throw new InvalidOperationException("Your license does not allow the use of the TimeSeries");
            }

            Authentication.AssertLicensedBundles(config.Core.ActiveBundles);
        }

        public void ForAllTimeSeries(Action<TimeSeriesStorage> action)
        {
            foreach (var value in ResourcesStoresCache
                .Select(cs => cs.Value)
                .Where(value => value.Status == TaskStatus.RanToCompletion))
            {
                action(value.Result);
            }
        }

        protected override DateTime LastWork(TimeSeriesStorage resource)
        {
            return resource.LastWrite;
        }
    }
}
