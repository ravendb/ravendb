// -----------------------------------------------------------------------
//  <copyright file="StandradBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Storage;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests
{
	public class IncrementalBackupTest : TransactionalStorageTestBase
	{
		private const string BackupDir = @".\BackupDatabase\";

		private class User
		{
			public string Name { get; set; }
		}

		public IncrementalBackupTest()
		{
			IOExtensions.DeleteDirectory(BackupDir);
            
		}

		[Theory(Timeout = 30000,Skip = "Not supported in Voron yet")]
        [PropertyData("Storages")]
		public void CreateIncrementalBackup(string storageName)
		{
            using (var store = NewDocumentStore(requestedStorage: storageName,runInMemory:false))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Fitzchak"});
					session.SaveChanges();
                }

                if (store.DocumentDatabase.TransactionalStorage.FriendlyName == "Munin")
                {
                    var exception = Assert.Throws<InvalidOperationException>(() => store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument()));
                    Assert.Contains("Backup operation is not supported when running in memory. ", exception.Message);
                    return;
                }

			    var indexDefinitionsFolder = Path.Combine(store.DocumentDatabase.Configuration.DataDirectory,"IndexDefinitions");
			    if (!Directory.Exists(indexDefinitionsFolder))
			        Directory.CreateDirectory(indexDefinitionsFolder);

			    Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument()));			    
				WaitForBackup(store.DocumentDatabase, true);

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Oren"});
					session.SaveChanges();
				}

				Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument()));
				WaitForBackup(store.DocumentDatabase, true);
			}
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/Esent/CircularLog"] = "false";
			configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(BackupDir);
			base.Dispose();
		}
	}
}