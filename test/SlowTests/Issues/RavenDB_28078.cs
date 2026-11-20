using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
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

        private static DatabaseRecord ApplyOnNewRecord<T>(BlittableJsonReaderObject json) where T : UpdateDatabaseCommand
        {
            var command = (T)JsonDeserializationCluster.Commands[typeof(T).Name](json);
            var record = new DatabaseRecord(command.DatabaseName);
            command.UpdateDatabaseRecord(record, 10);
            return record;
        }

    }
}
