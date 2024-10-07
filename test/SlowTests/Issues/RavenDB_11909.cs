﻿using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11909 : RavenTestBase
    {
        public RavenDB_11909(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CreateDatabaseOperationShouldThrowOnPassedTaskConfigurations(Options options)
        {
            var dbName1 = $"db1_{Guid.NewGuid().ToString()}";

            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ThrowOnUpdateDatabaseRecordContainsTasksConfigurations(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, replicationFactor: 1, etag)));
            }
        }

        [Fact]
        public void ThrowOnDatabaseRecordChanges()
        {
            const int numberOfFields = 45;
            const int numberOfProperties = 1;

            var tasksList = new List<string>
            {
                nameof(DatabaseRecord.PeriodicBackups),
                nameof(DatabaseRecord.ExternalReplications),
                nameof(DatabaseRecord.SinkPullReplications),
                nameof(DatabaseRecord.HubPullReplications),
                nameof(DatabaseRecord.RavenEtls),
                nameof(DatabaseRecord.SqlEtls),
                nameof(DatabaseRecord.OlapEtls),
                nameof(DatabaseRecord.ElasticSearchEtls),
                nameof(DatabaseRecord.QueueEtls),
                nameof(DatabaseRecord.QueueSinks),
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

            var integrationConfigsType = typeof(IntegrationConfigurations);

            const int numberOfIntegrationFields = 1;
            const int numberOfIntegrationProperties = 0;

            var integrationFields = integrationConfigsType.GetFields().Select(x => x.Name);
            var integrationProperties = integrationConfigsType.GetProperties().Select(x => x.Name);

            // check if new fields or properties has been added to IntegrationConfigurations inside DatabaseRecord
            Assert.True(numberOfIntegrationFields == integrationFields.Count(),
                $"New fields has been added to {nameof(IntegrationConfigurations)} please bump {nameof(numberOfIntegrationFields)} and update the {nameof(tasksList)} in {nameof(ClusterStateMachine)}.AddDatabase() and here if necessary");

            Assert.True(numberOfIntegrationProperties == integrationProperties.Count(),
                $"New properties has been added to {nameof(IntegrationConfigurations)} please bump {nameof(numberOfIntegrationProperties)} and update the {nameof(tasksList)} in {nameof(ClusterStateMachine)}.AddDatabase() and here if necessary");
        }
    }
}
