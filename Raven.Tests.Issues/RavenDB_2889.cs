// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2808.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2889 : ReplicationBase
	{
		[Fact]
		public void DuringRestoreDatabaseIdCanBeChanged()
		{
			var backupPath = NewDataPath();

			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "N1", 
						Settings =
						{
							{ Constants.ActiveBundles, "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				var commands = store.DatabaseCommands.ForDatabase("N1");
				var oldDatabaseId = store.DatabaseCommands.GetStatistics().DatabaseId;

				commands.GlobalAdmin.StartBackup(backupPath, null, incremental: false, databaseName: "N1");

				WaitForBackup(commands, true);

				var operation = commands
					.GlobalAdmin
					.StartRestore(new DatabaseRestoreRequest
					{
						BackupLocation = backupPath,
						DatabaseName = "N3",
						GenerateNewDatabaseId = true
					});

				var status = operation.WaitForCompletion();

				var newDatabaseId = commands
					.ForDatabase("N3")
					.GetStatistics()
					.DatabaseId;

				Assert.NotEqual(oldDatabaseId, newDatabaseId);
			}
		}

		[Fact]
		public void AfterRestoreDatabaseIdIsTheSame()
		{
			var backupPath = NewDataPath();

			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "N1",
						Settings =
						{
							{ Constants.ActiveBundles, "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				var commands = store.DatabaseCommands.ForDatabase("N1");
				var oldDatabaseId = commands.GetStatistics().DatabaseId;

				commands.GlobalAdmin.StartBackup(backupPath, null, incremental: false, databaseName: "N1");

				WaitForBackup(commands, true);

				var operation = commands
					.GlobalAdmin
					.StartRestore(new DatabaseRestoreRequest
					{
						BackupLocation = backupPath,
						DatabaseName = "N3",
						GenerateNewDatabaseId = false
					});

				var status = operation.WaitForCompletion();

				var newDatabaseId = commands
					.ForDatabase("N3")
					.GetStatistics()
					.DatabaseId;

				Assert.Equal(oldDatabaseId, newDatabaseId);
			}
		}
	}
}