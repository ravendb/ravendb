using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11909 : RavenTestBase
    {
        public RavenDB_11909(ITestOutputHelper output) : base(output)
        {
        }

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

        [Fact]
        public void ThrowOnDatabaseRecordChanges()
        {
            const int numberOfFields = 33;
            const int numberOfProperties = 1;
            var tasksList = new List<string>
            {
                nameof(DatabaseRecord.PeriodicBackups),
                nameof(DatabaseRecord.ExternalReplications),
                nameof(DatabaseRecord.SinkPullReplications),
                nameof(DatabaseRecord.HubPullReplications),
                nameof(DatabaseRecord.RavenEtls),
                nameof(DatabaseRecord.SqlEtls)
            };

            var dbRecordType = typeof(DatabaseRecord);
            var fields = dbRecordType.GetFields().Select(x => x.Name);
            var properties = dbRecordType.GetProperties().Select(x => x.Name);

            // check if deleted fields from our list
            Assert.True(tasksList.All(val => fields.Contains(val)), 
                $"Some configuration deleted from {nameof(DatabaseRecord)} please update the {nameof(tasksList)} in {nameof(ClusterStateMachine)}.AddDatabase() and here.");
           
            // check if new fields or properties has been added to DatabaseRecord
            Assert.True(numberOfFields == fields.Count(), 
                $"New fields has been added to {nameof(DatabaseRecord)} please bump {nameof(numberOfFields)} and update the {nameof(tasksList)} in {nameof(ClusterStateMachine)}.AddDatabase() and here if necessary");

            Assert.True(numberOfProperties == properties.Count(),
                $"New properties has been added to {nameof(DatabaseRecord)} please bump {nameof(numberOfProperties)} and update the {nameof(tasksList)} in {nameof(ClusterStateMachine)}.AddDatabase() and here if necessary");
        }
    }
}
