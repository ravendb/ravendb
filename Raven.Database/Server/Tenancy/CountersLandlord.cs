// -----------------------------------------------------------------------
//  <copyright file="CountersLandlord.cs" company="Hibernating Rhinos LTD">
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
using Raven.Database.Counters;
using Raven.Database.Server.Connections;

namespace Raven.Database.Server.Tenancy
{
	public class CountersLandlord : AbstractLandlord<CounterStorage>
	{
		private readonly DocumentDatabase systemDatabase;
		private bool initialized;

        private const string COUNTERS_PREFIX = "Raven/Counters/";
        public override string ResourcePrefix { get { return COUNTERS_PREFIX; } }

		public CountersLandlord(DocumentDatabase systemDatabase)
		{
			this.systemDatabase = systemDatabase;
		    Enabled = systemDatabase.Documents.Get("Raven/Counters/Enabled",null) != null;
			Init();
		}

	    public bool Enabled { get; set; }

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
                const string ravenDbPrefix = "Raven/Counters/";
                if (notification.Id.StartsWith(ravenDbPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;
                var dbName = notification.Id.Substring(ravenDbPrefix.Length);
                Logger.Info("Shutting down counters {0} because the tenant counter document has been updated or removed", dbName);
				Cleanup(dbName, skipIfActive: false, notificationType: notification.Type);
            };
        }

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId)
		{
			if (string.IsNullOrWhiteSpace(tenantId))
				throw new ArgumentException("tenantId");
			var document = GetTenantDatabaseDocument(tenantId);
			if (document == null)
				return null;

			return CreateConfiguration(tenantId, document, "Raven/Counters/DataDir", systemDatabase.Configuration);
		}

		private DatabaseDocument GetTenantDatabaseDocument(string tenantId)
		{
			JsonDocument jsonDocument;
			using (systemDatabase.DisableAllTriggersForCurrentThread())
				jsonDocument = systemDatabase.Documents.Get("Raven/Counters/" + tenantId, null);
			if (jsonDocument == null ||
				jsonDocument.Metadata == null ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
				return null;

			var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (document.Settings.Keys.Contains("Raven/Counters/DataDir") == false)
				throw new InvalidOperationException("Could not find Raven/Counters/DataDir");

			if (document.Disabled)
				throw new InvalidOperationException("The database has been disabled.");

			return document;
		}

		public bool TryGetOrCreateResourceStore(string tenantId, out Task<CounterStorage> counter)
		{
			if (Locks.Contains(DisposingLock))
				throw new ObjectDisposedException("CountersLandlord", "Server is shutting down, can't access any counters");

		    if (Enabled == false)
		    {
                throw new InvalidOperationException("Counters are an experimental feature that is not enabled");
		    }
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

			if (Locks.Contains(tenantId))
				throw new InvalidOperationException("Counters '" + tenantId + "' is currently locked and cannot be accessed");

			var config = CreateTenantConfiguration(tenantId);
			if (config == null)
				return false;

			counter = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
			{
				var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());
				var cs = new CounterStorage(systemDatabase.ServerUrl, tenantId, config, transportState);
				AssertLicenseParameters();

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

		private void AssertLicenseParameters()
		{
			string maxDatabases;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfCounters", out maxDatabases))
			{
				if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
				{
					var numberOfAllowedFileSystems = int.Parse(maxDatabases);

					int nextPageStart = 0;
					var databases =
						systemDatabase.Documents.GetDocumentsWithIdStartingWith("Raven/Counters/", null, null, 0,
							numberOfAllowedFileSystems, CancellationToken.None, ref nextPageStart).ToList();
					if (databases.Count >= numberOfAllowedFileSystems)
						throw new InvalidOperationException(
							"You have reached the maximum number of counters that you can have according to your license: " +
							numberOfAllowedFileSystems + Environment.NewLine +
							"You can either upgrade your RavenDB license or delete a counter from the server");
				}
			}
		}


		public async Task<CounterStorage> GetCounterInternal(string name)
		{
			Task<CounterStorage> db;
			if (TryGetOrCreateResourceStore(name, out db))
				return await db;
			return null;
		}

		public void ForAllCounters(Action<CounterStorage> action)
		{
			foreach (var value in ResourcesStoresCache
				.Select(db => db.Value)
				.Where(value => value.Status == TaskStatus.RanToCompletion))
			{
				action(value.Result);
			}
		}

		protected override DateTime LastWork(CounterStorage resource)
		{
			return resource.LastWrite;
		}
	}
}