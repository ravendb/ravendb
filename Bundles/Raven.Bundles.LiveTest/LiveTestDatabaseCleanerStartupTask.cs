// -----------------------------------------------------------------------
//  <copyright file="LiveTestDatabaseCleanerStartupTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Configuration;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Bundles.LiveTest
{
	public class LiveTestDatabaseCleanerStartupTask : IServerStartupTask
	{
		private Timer checkTimer;

		private RavenDbServer server;

		private TimeSpan maxTimeDatabaseCanBeIdle;

		public void Execute(RavenDbServer server)
		{
			this.server = server;

			int val;
			if (int.TryParse(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;

			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val); ;

			checkTimer = new Timer(ExecuteCleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10));
		}

		public void Dispose()
		{
			if (checkTimer != null)
				checkTimer.Dispose();
		}

		public void ExecuteCleanup(object state)
		{
			var databaseLandLord = server.Options.DatabaseLandlord;
			var systemDatabase = databaseLandLord.SystemDatabase;
			if (server.Disposed)
			{
				Dispose();
				return;
			}

			int nextStart = 0;
			var databaseDocuments = systemDatabase
				.Documents
				.GetDocumentsWithIdStartingWith(Constants.RavenDatabasesPrefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);

			var databaseIds = databaseDocuments
				.Select(x => ((RavenJObject)x)["@metadata"])
				.Where(x => x != null)
				.Select(x => x.Value<string>("@id"))
				.Where(x => x != null && x != Constants.SystemDatabase)
				.ToList();

			foreach (var databaseId in databaseIds)
			{
				var key = databaseId;
				if (key.StartsWith(Constants.RavenDatabasesPrefix))
					key = key.Substring(Constants.RavenDatabasesPrefix.Length);

				var shouldCleanup = false;

				DateTime value;
				if (databaseLandLord.IsDatabaseLoaded(key) == false)
					shouldCleanup = true;
				else if (databaseLandLord.LastRecentlyUsed.TryGetValue(key, out value) == false || (SystemTime.UtcNow - value) > maxTimeDatabaseCanBeIdle)
					shouldCleanup = true;

				if (shouldCleanup == false)
					continue;

				var configuration = databaseLandLord.CreateTenantConfiguration(key, true);

				databaseLandLord.Cleanup(key, maxTimeDatabaseCanBeIdle, database => false);

				var docKey = Constants.RavenDatabasesPrefix + key;
				systemDatabase.Documents.Delete(docKey, null, null);

				if (configuration == null)
					continue;

				IOExtensions.DeleteDirectory(configuration.DataDirectory);
				if (configuration.IndexStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

				if (configuration.Storage.Esent.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.Storage.Esent.JournalsStoragePath);

				if (configuration.Storage.Voron.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.Storage.Voron.JournalsStoragePath);
			}
		}
	}
}