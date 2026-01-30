using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_25471 : ReplicationTestBase
    {
        public RavenDB_25471(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { BackupType.Backup })]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { BackupType.Snapshot })]
        public async Task DisableOlapOnRestoreWithoutLicense(Options options, BackupType backupType)
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var source = GetDocumentStore();
            await DisableRevisionCompression(Server, source);
            SetupLocalOlapEtl(source, script: @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
", path: NewDataPath());

            var backupOperation = await source.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = backupType,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));
            await backupOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            await source.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(source.Database, hardDelete: true));

            await PutLicense(Server, LicenseTestBase.RL_COMM);

            using var store = GetDocumentStore();

            using var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = source.Database + "_Restore" }.Initialize();

            using (Backup.RestoreDatabase(destination,
                       new RestoreBackupConfiguration
                       {
                           BackupLocation = Directory.GetDirectories(backupPath).First(),
                           DatabaseName = destination.Database,
                           DisableOngoingTasks = true // <<==
                       })) // restore shouldnt throw
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(destination.Database));
                Assert.Equal(1, record.OlapEtls.Count);
                foreach (var olap in record.OlapEtls)
                {
                    Assert.True(olap.Disabled);
                }
            }
        }

        private void SetupLocalOlapEtl(DocumentStore store, string script, string path)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                ConnectionStringName = connectionStringName,
                RunFrequency = "* * * * *",
                Transforms =
            {
                new Transformation
                {
                    Name = "MonthlyOrders",
                    Collections = new List<string> {"Orders"},
                    Script = script
                }
            }
            };

            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            Etl.AddEtl(store, configuration, connectionString);
        }

        private static async Task PutLicense(RavenServer leader, string licenseType)
        {
            var license = Environment.GetEnvironmentVariable(licenseType);
            Raven.Server.Commercial.LicenseHelper.TryDeserializeLicense(license, out License li);

            await leader.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId());
        }

        private static async Task DisableRevisionCompression(RavenServer leader, DocumentStore store)
        {
            var command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store.Database,
                RaftIdGenerator.NewId());
            await leader.ServerStore.SendToLeaderAsync(command);
        }
    }
}
