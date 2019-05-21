using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11909 : RavenTestBase
    {
        [Fact]
        public void CreateDatabaseOperationShouldThrowOnPassedTaskConfigurations()
        {
            var dbName1 = $"db1_{Guid.NewGuid().ToString()}";

            using (var store = GetDocumentStore())
            {
                var dbRecord = new DatabaseRecord(dbName1)
                {
                    ExternalReplications = new List<ExternalReplication>
                    {
                        new ExternalReplication(dbName1, store.Database)
                    }
                };
                Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord)));

                dbRecord = new DatabaseRecord(dbName1)
                {
                    PeriodicBackups = new List<PeriodicBackupConfiguration>
                    {
                        new PeriodicBackupConfiguration
                        {
                            LocalSettings = new LocalSettings
                            {
                                FolderPath = ""
                            },
                            FullBackupFrequency = "* */1 * * *",
                            IncrementalBackupFrequency = "* */2 * * *"
                        }
                    }
                };
                Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord)));
            }
        }

        [Fact]
        public void ThrowOnUpdateDatabaseRecordContainsTasksConfigurations()
        {
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

                var etag = record.Etag;
                Assert.NotNull(record);
                Assert.True(etag > 0);

                record.PeriodicBackups = new List<PeriodicBackupConfiguration>
                {
                    new PeriodicBackupConfiguration
                    {
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = ""
                        },
                        FullBackupFrequency = "* */1 * * *",
                        IncrementalBackupFrequency = "* */2 * * *"
                    }
                };

                Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, etag)));
            }
        }
    }
}
