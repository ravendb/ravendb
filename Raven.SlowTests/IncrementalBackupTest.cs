// -----------------------------------------------------------------------
//  <copyright file="StandradBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.SlowTests
{
    public class IncrementalBackupTest : RavenTest
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

        [Theory(Timeout = 60000)]
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

                var indexDefinitionsFolder = Path.Combine(store.SystemDatabase.Configuration.DataDirectory,"IndexDefinitions");
                if (!Directory.Exists(indexDefinitionsFolder))
                    Directory.CreateDirectory(indexDefinitionsFolder);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));			    
                WaitForBackup(store.SystemDatabase, true);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Oren"});
                    session.SaveChanges();
                }

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);
            }
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/Esent/CircularLog"] = "false";
            configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "true";
            configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
            configuration.Initialize();
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory(BackupDir);
            base.Dispose();
        }
    }
}
