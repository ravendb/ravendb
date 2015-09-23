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
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Tenancy
{
	public class TimeSeriesLandlord : AbstractLandlord<TimeSeriesStorage>
	{
		private bool initialized;

		public override string ResourcePrefix { get { return Constants.TimeSeries.Prefix; } }

		public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };

		public TimeSeriesLandlord(DocumentDatabase systemDatabase)
			: base(systemDatabase)
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
				Logger.Info("Shutting down time series {0} because the tenant time series document has been updated or removed", dbName);
				Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
			};
		}

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledTimeSeries = false)
		{
			if (string.IsNullOrWhiteSpace(tenantId))
				throw new ArgumentException("tenantId");
			var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledTimeSeries);
			if (document == null)
				return null;

			return CreateConfiguration(tenantId, document, Constants.TimeSeries.DataDirectory, systemDatabase.Configuration);
		}

		protected InMemoryRavenConfiguration CreateConfiguration(
						string tenantId,
						TimeSeriesDocument document,
						string folderPropName,
						InMemoryRavenConfiguration parentConfiguration)
		{
			var config = new InMemoryRavenConfiguration
			{
				Settings = new NameValueCollection(parentConfiguration.Settings),
			};

			SetupTenantConfiguration(config);

			config.CustomizeValuesForDatabaseTenant(tenantId);

			config.Settings[Constants.TimeSeries.DataDirectory] = parentConfiguration.TimeSeries.DataDirectory;
			//config.Settings["Raven/StorageEngine"] = parentConfiguration.DefaultStorageTypeName;
			//TODO: what time series dir path?
			//config.Settings["Raven/TimeSeries/Storage"] = parentConfiguration.FileSystem.DefaultStorageTypeName;

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
			//config.Settings["Raven/Esent/LogsPath"] = config.Settings["Raven/Esent/LogsPath"].ToFullPath(parentConfiguration.DataDirectory);
			config.Settings[Constants.RavenTxJournalPath] = config.Settings[Constants.RavenTxJournalPath].ToFullPath(parentConfiguration.DataDirectory);

			config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;
			config.TimeSeriesName = tenantId;

			config.Initialize();
			config.CopyParentSettings(parentConfiguration);
			return config;
		}

		private TimeSeriesDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledTimeSeriesStorage = false)
		{
			JsonDocument jsonDocument;
			using (systemDatabase.DisableAllTriggersForCurrentThread())
				jsonDocument = systemDatabase.Documents.Get(ResourcePrefix + tenantId, null);
			if (jsonDocument == null || jsonDocument.Metadata == null ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
				return null;

			var document = jsonDocument.DataAsJson.JsonDeserialization<TimeSeriesDocument>();
			if (document.Settings.Keys.Contains(Constants.TimeSeries.DataDirectory) == false)
				throw new InvalidOperationException("Could not find " + Constants.TimeSeries.DataDirectory);

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
			if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxSecondsForTaskToWaitForDatabaseToLoad) == false)
				throw new InvalidOperationException(string.Format("TimeSeries '{0}' are currently being restarted and cannot be accessed. We already waited {1} seconds.", tenantId, MaxSecondsForTaskToWaitForDatabaseToLoad));

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

		private void AssertLicenseParameters(InMemoryRavenConfiguration config)
		{
			string maxDatabases;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfTimeSeries", out maxDatabases))
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
							"You have reached the maximum number of time series that you can have according to your license: " +
							numberOfAllowedFileSystems + Environment.NewLine +
							"You can either upgrade your RavenDB license or delete a timeSeries from the server");
				}
			}

			if (Authentication.IsLicensedForTimeSeries == false)
			{
				throw new InvalidOperationException("Your license does not allow the use of the TimeSeries");
			}

			Authentication.AssertLicensedBundles(config.ActiveBundles);
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