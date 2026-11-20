using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_28078 : ClusterTestBase
    {
        public RavenDB_28078(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.BackupExportImport)]
        public void NewPeriodicBackupTaskGetsCreatedAtFromSendingNode()
        {
            var command = new UpdatePeriodicBackupCommand(new PeriodicBackupConfiguration { Name = "backup", FullBackupFrequency = "0 * * * *" }, "db", "id");
            var createdAt = command.Configuration.CreatedAt;
            Assert.NotNull(createdAt);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(command.ToJson(context), "command");

                var first = ApplyOnNewRecord<UpdatePeriodicBackupCommand>(json);
                Thread.Sleep(20);
                var second = ApplyOnNewRecord<UpdatePeriodicBackupCommand>(json);

                Assert.Equal(createdAt, first.PeriodicBackups[0].CreatedAt);
                Assert.Equal(createdAt, second.PeriodicBackups[0].CreatedAt);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Configuration, LicenseRequired = true)]
        public async Task SchemaDefinitionKeepsLastModifiedTimeWhenTheRecordIsRewritten()
        {
            using var store = GetDocumentStore();

            var definition = new SchemaDefinition { Schema = "{\"properties\":{\"Name\":{\"type\":\"string\"}}}" };
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition> { ["Users"] = definition }
            }));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(definition.LastModifiedTime, record.SchemaValidation.ValidatorsPerCollection["Users"].LastModifiedTime);

            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 60 }));

            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(definition.LastModifiedTime, record.SchemaValidation.ValidatorsPerCollection["Users"].LastModifiedTime);
        }

        private static DatabaseRecord ApplyOnNewRecord<T>(BlittableJsonReaderObject json) where T : UpdateDatabaseCommand
        {
            var command = (T)JsonDeserializationCluster.Commands[typeof(T).Name](json);
            var record = new DatabaseRecord(command.DatabaseName);
            command.UpdateDatabaseRecord(record, 10);
            return record;
        }

    }
}
