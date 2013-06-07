// -----------------------------------------------------------------------
//  <copyright file="RavenDB_865.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_865 : RavenTest
	{
		private const string BackupDir = @".\BackupDatabase\";
		private const string RestoreDir = @".\RestoredDatabase\";
		private const string RestoredDatabaseName = "Database-865-Restore";

		[Fact]
		public async Task Restore_operation_works_async()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				store.DatabaseCommands.Put("keys/1", null, new RavenJObject() { { "Key", 1 } }, new RavenJObject());

				await store.AsyncDatabaseCommands.StartBackupAsync(BackupDir, new DatabaseDocument());

				WaitForBackup(store.DatabaseCommands, true);

				// restore as a new database
				await store.AsyncDatabaseCommands.StartRestoreAsync(BackupDir, RestoreDir, RestoredDatabaseName);

				// get restore status and wait for finish
				var done = SpinWait.SpinUntil(() =>
				{
					var doc = store.DatabaseCommands.Get(RestoreStatus.RavenRestoreStatusDocumentKey);

					if (doc == null)
						return false;

					var status = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();

					return status.Last().Contains("The new database was created");
				}, TimeSpan.FromMinutes(5));

				Assert.True(done);

				Assert.Equal(1, store.DatabaseCommands.ForDatabase(RestoredDatabaseName).Get("keys/1").DataAsJson.Value<int>("Key"));
			}
		}

		public override void Dispose()
		{
			base.Dispose();
			IOExtensions.DeleteDirectory(BackupDir);
			IOExtensions.DeleteDirectory(RestoreDir);
		}
	}
}