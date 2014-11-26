// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2808.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
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

		[Fact]
		public void SmugglerCanStripReplicationInformationDuringImport()
		{
			var path = NewDataPath(forceCreateDir: true);
			var backupPath = Path.Combine(path, "backup.dump");

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
				commands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
				var doc = commands.Get("keys/1");
				Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));

				commands.PutAttachment("keys/1", null, new MemoryStream(), new RavenJObject());
				var attachment = commands.GetAttachment("keys/1");
				Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));

				var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions { StripReplicationInformation = true });
				smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
				{
					ToFile = backupPath,
					From = new RavenConnectionStringOptions
					{
						Url = store.Url,
						DefaultDatabase = "N1"
					}
				}).Wait(TimeSpan.FromSeconds(15));

				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "N2",
						Settings =
						{
							{ Constants.ActiveBundles, "" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
									{
										FromFile = backupPath,
										To = new RavenConnectionStringOptions
										{
											DefaultDatabase = "N2",
											Url = store.Url
										}
									}).Wait(TimeSpan.FromSeconds(15));

				commands = store.DatabaseCommands.ForDatabase("N2");
				doc = commands.Get("keys/1");
				Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
				attachment = commands.GetAttachment("keys/1");
				Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));

				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "N3",
						Settings =
						{
							{ Constants.ActiveBundles, "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
				{
					FromFile = backupPath,
					To = new RavenConnectionStringOptions
					{
						DefaultDatabase = "N3",
						Url = store.Url
					}
				}).Wait(TimeSpan.FromSeconds(15));

				commands = store.DatabaseCommands.ForDatabase("N3");
				doc = commands.Get("keys/1");
				Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
				attachment = commands.GetAttachment("keys/1");
				Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
				Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));
			}
		}
	}
}