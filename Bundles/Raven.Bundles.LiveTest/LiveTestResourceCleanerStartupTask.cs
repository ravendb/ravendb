// -----------------------------------------------------------------------
//  <copyright file="LiveTestResourceCleanerStartupTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Configuration;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Bundles.LiveTest
{
	public class LiveTestResourceCleanerStartupTask : IServerStartupTask
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private RavenDbServer server;

		private TimeSpan maxTimeResourceCanBeIdle;

		public void Execute(RavenDbServer ravenDbServer)
		{
			server = ravenDbServer;

			int val;
			if (int.TryParse(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Tenants/MaxIdleTimeForTenantResource"], out val) == false)
				val = 900;

			maxTimeResourceCanBeIdle = TimeSpan.FromSeconds(val);

			log.Info("LiveTestResourceCleanerStartupTask started. MaxTimeResourceCanBeIdle: " + maxTimeResourceCanBeIdle.TotalSeconds + " seconds.");

			server.Options.SystemDatabase.TimerManager.NewTimer(ExecuteCleanup, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10));
		}

		public void Dispose()
		{
		}

		public void ExecuteCleanup(object state)
		{
			log.Info("LiveTestResourceCleanerStartupTask. Executing cleanup.");

			var databaseLandlord = server.Options.DatabaseLandlord;
			var fileSystemLandlord = server.Options.FileSystemLandlord;

			if (server.Disposed)
			{
				Dispose();
				return;
			}

			try
			{
				CleanupDatabases(databaseLandlord);
				CleanupFileSystems(databaseLandlord, fileSystemLandlord);
			}
			catch (Exception e)
			{
				log.ErrorException("Unexpected error.", e);
			}
		}

		private void CleanupFileSystems(DatabasesLandlord databaseLandlord, FileSystemsLandlord fileSystemLandlord)
		{
			var systemDatabase = databaseLandlord.SystemDatabase;

			int nextStart = 0;
			var fileSystemDocuments = systemDatabase
				.Documents
                .GetDocumentsWithIdStartingWith(Constants.FileSystem.Prefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);

			var fileSystemIds = fileSystemDocuments
				.Select(x => ((RavenJObject)x)["@metadata"])
				.Where(x => x != null)
				.Select(x => x.Value<string>("@id"))
				.Where(x => x != null && x != Constants.SystemDatabase)
				.ToList();

			foreach (var fileSystemId in fileSystemIds)
			{
				try
				{
					var key = fileSystemId;
                    if (key.StartsWith(Constants.FileSystem.Prefix))
                        key = key.Substring(Constants.FileSystem.Prefix.Length);

					var shouldCleanup = false;

					DateTime value;
					if (fileSystemLandlord.IsFileSystemLoaded(key) == false) 
						shouldCleanup = true;
					else if (fileSystemLandlord.LastRecentlyUsed.TryGetValue(key, out value) == false || (SystemTime.UtcNow - value) > maxTimeResourceCanBeIdle) 
						shouldCleanup = true;

					if (shouldCleanup == false) 
						continue;

					var configuration = fileSystemLandlord.CreateTenantConfiguration(key, true);

					fileSystemLandlord.Cleanup(key, maxTimeResourceCanBeIdle, database => false);

                    var docKey = Constants.FileSystem.Prefix + key;
					systemDatabase.Documents.Delete(docKey, null, null);

					if (configuration == null)
						continue;

					IOExtensions.DeleteDirectory(configuration.FileSystem.DataDirectory);
				}
				catch (Exception e)
				{
					log.WarnException(string.Format("Failed to cleanup '{0}' filesystem.", fileSystemId), e);
				}
			}
		}

		private void CleanupDatabases(DatabasesLandlord databaseLandlord)
		{
			var systemDatabase = databaseLandlord.SystemDatabase;

			int nextStart = 0;
			var databaseDocuments = systemDatabase
				.Documents
				.GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);

			var databaseIds = databaseDocuments
				.Select(x => ((RavenJObject)x)["@metadata"])
				.Where(x => x != null)
				.Select(x => x.Value<string>("@id"))
				.Where(x => x != null && x != Constants.SystemDatabase)
				.ToList();

			foreach (var databaseId in databaseIds)
			{
				try
				{
					var key = databaseId;
					if (key.StartsWith(Constants.Database.Prefix))
						key = key.Substring(Constants.Database.Prefix.Length);

					var shouldCleanup = false;

					DateTime value;
					if (databaseLandlord.IsDatabaseLoaded(key) == false)
						shouldCleanup = true;
					else if (databaseLandlord.LastRecentlyUsed.TryGetValue(key, out value) == false || (SystemTime.UtcNow - value) > maxTimeResourceCanBeIdle)
						shouldCleanup = true;

					if (shouldCleanup == false)
						continue;

					var configuration = databaseLandlord.CreateTenantConfiguration(key, true);

					databaseLandlord.Cleanup(key, maxTimeResourceCanBeIdle, database => false);

					var docKey = Constants.Database.Prefix + key;
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
				catch (Exception e)
				{
					log.WarnException(string.Format("Failed to cleanup '{0}' database.", databaseId), e);
				}
			}
		}
	}
}