using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task ClusterTransactionResultIsTheSameOnAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "Oren" }, "users/1");
                await session.SaveChangesAsync();
            }

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(leader.ServerStore.LastRaftCommitIndex, nodes);

            var results = nodes.Select(GetClusterTransactionResults).ToList();
            Assert.Single(results[0]);
            Assert.All(results, x => Assert.Equal(results[0], x));
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Certificates)]
        public async Task ReplicationHubAccessWithSameKeyAndExpirationDoesNotStopTheStateMachine()
        {
            using var store = GetDocumentStore();

            using var key = RSA.Create(2048);
            var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
            var notAfter = notBefore.AddYears(1);
            using var first = new CertificateRequest("CN=sink-1", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1).CreateSelfSigned(notBefore, notAfter);
            using var second = new CertificateRequest("CN=sink-2", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1).CreateSelfSigned(notBefore, notAfter);

            await Server.ServerStore.SendToLeaderAsync(new RegisterReplicationHubAccessCommand(store.Database, "hub", CreateAccess(first), first, RaftIdGenerator.NewId()));
            await Server.ServerStore.SendToLeaderAsync(new RegisterReplicationHubAccessCommand(store.Database, "hub", CreateAccess(second), second, RaftIdGenerator.NewId())
            {
                RegisteringSamePublicKeyPinningHash = true
            });

            var result = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key", "value", 0));
            Assert.True(result.Successful);

            static ReplicationHubAccess CreateAccess(X509Certificate2 certificate) => new ReplicationHubAccess
            {
                Name = "sink",
                CertificateBase64 = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = ["*"],
                AllowedSinkToHubPaths = ["*"]
            };
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Certificates)]
        public void InstallUpdatedServerCertificateCommandCarriesTheOldThumbprintOfTheSendingNode()
        {
            var command = new InstallUpdatedServerCertificateCommand("certificate", "old-thumbprint", replaceImmediately: false, "id");

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(command.ToJson(context), "command");

                Assert.True(json.TryGet(nameof(InstallUpdatedServerCertificateCommand.OldThumbprint), out string oldThumbprint));
                Assert.Equal("old-thumbprint", oldThumbprint);
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void DeleteDatabaseCommandUsesTheClusterNodesOfTheSendingNode()
        {
            var sent = new DeleteDatabaseCommand("db", "id") { ClusterNodes = ["B"] };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(sent.ToJson(context), "command");
                var command = (DeleteDatabaseCommand)JsonDeserializationCluster.Commands[nameof(DeleteDatabaseCommand)](json);

                using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterContext))
                using (clusterContext.OpenReadTransaction())
                    command.Initialize(Server.ServerStore, clusterContext);

                var record = new DatabaseRecord("db")
                {
                    Topology = new DatabaseTopology { Members = new List<string> { "B", "C" } }
                };
                command.UpdateDatabaseRecord(record, 10);

                Assert.Equal(["B"], record.DeletionInProgress.Keys);
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task DatabaseTopologyStampHasTheTermOfTheLogEntryOnAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
            var stopped = nodes.First(x => x != leader);
            var running = nodes.Where(x => x != stopped).ToList();
            var stoppedInfo = await DisposeServerAndWaitForFinishOfDisposalAsync(stopped);

            var database = GetDatabaseName();
            using (var store = new DocumentStore { Urls = [leader.WebUrl], Database = database }.Initialize())
            {
                var result = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)
                {
                    Topology = new DatabaseTopology { Members = running.Select(x => x.ServerStore.NodeTag).ToList() }
                }));

                var term = leader.ServerStore.Engine.CurrentTerm;
                leader.ServerStore.Engine.CurrentLeader?.StepDown();
                await WaitForValueAsync(() => running.Any(x => x.ServerStore.IsLeader() && x.ServerStore.Engine.CurrentTerm > term), true);

                var revived = await ReviveNodeAsync(stoppedInfo);
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.RaftCommandIndex, running.Append(revived).ToList());

                var terms = running.Append(revived).Select(x => GetTopologyStampTerm(x, database)).ToList();
                Assert.All(terms, x => Assert.Equal(terms[0], x));
            }
        }

        private static long GetTopologyStampTerm(RavenServer server, string database)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                return server.ServerStore.Cluster.ReadDatabase(context, database).Topology.Stamp.Term;
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeExpirationWithoutATimeZoneIsUtcOnAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            // as an older node sends them: the value keeps the time the client wrote
            await PutCompareExchangeAsIsAsync(leader, store.Database, "users/no-time-zone", "2030-01-01T00:00:00.0000000");
            await PutCompareExchangeAsIsAsync(leader, store.Database, "users/offset", "2030-01-01T05:00:00.0000000+05:00");
            await WaitForAllNodesToApplyAsync(nodes);

            var expected = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
            Assert.All(nodes, node => Assert.Equal(new[] { expected, expected }, GetCompareExchangeExpirationTicks(node)));
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeWhoseExpirationEntryIsEarlierThanItsValueIsDeletedAtItsValue()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            await PutCompareExchangeAsIsAsync(leader, store.Database, "users/1", "2030-01-01T00:00:00.0000000Z");
            await WaitForAllNodesToApplyAsync(nodes);

            // an older version computed the entry with the local time zone of the node, here UTC+5
            var expires = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
            var entry = expires - TimeSpan.FromHours(5).Ticks;
            foreach (var node in nodes)
                MoveCompareExchangeExpirationEntry(node, expires, entry);

            await leader.ServerStore.SendToLeaderAsync(new DeleteExpiredCompareExchangeCommand(entry + 1, take: 100, RaftIdGenerator.NewId()));
            await WaitForAllNodesToApplyAsync(nodes);
            Assert.All(nodes, node => Assert.Equal(new[] { expires }, GetCompareExchangeExpirationTicks(node)));
            Assert.NotNull(await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("users/1")));

            await leader.ServerStore.SendToLeaderAsync(new DeleteExpiredCompareExchangeCommand(expires + 1, take: 100, RaftIdGenerator.NewId()));
            await WaitForAllNodesToApplyAsync(nodes);
            Assert.All(nodes, node => Assert.Empty(GetCompareExchangeExpirationTicks(node)));
            Assert.Null(await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("users/1")));
        }

        private static async Task PutCompareExchangeAsIsAsync(RavenServer server, string database, string key, string expires)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var value = context.ReadObject(new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Expires] = expires }
                }, key);
                await server.ServerStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeCommand(database, key, value, 0, context, RaftIdGenerator.NewId()));
            }
        }

        private static void MoveCompareExchangeExpirationEntry(RavenServer server, long from, long to)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var (key, _, ticks) = CompareExchangeExpirationStorage.GetExpiredValues(context, from).Single(x => x.expiredTicks == from);
                context.Transaction.InnerTransaction.ReadTree(CompareExchangeExpirationStorage.CompareExchangeByExpiration).MultiDelete(ticks, key);
                CompareExchangeExpirationStorage.Put(context, key, to);
                tx.Commit();
            }
        }

        private async Task WaitForAllNodesToApplyAsync(List<RavenServer> nodes)
        {
            var index = nodes.Max(x => x.ServerStore.LastRaftCommitIndex);
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(index, nodes);
        }

        private static List<long> GetCompareExchangeExpirationTicks(RavenServer server)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
                return CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue).Select(x => x.expiredTicks).ToList();
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.BackupExportImport)]
        [InlineData("/backups", "/backups/db")]
        [InlineData("/backups/", "/backups/db")]
        [InlineData("C:\\backups", "C:\\backups\\db")]
        [InlineData("C:\\backups\\", "C:\\backups\\db")]
        [InlineData("C:/backups", "C:/backups/db")]
        public void ServerWideBackupLocalFolderDoesNotDependOnTheOperatingSystem(string folderPath, string expected)
        {
            var localSettings = new LocalSettings { FolderPath = folderPath };

            PutServerWideBackupConfigurationCommand.UpdateSettingsForLocal(localSettings, "db");

            Assert.Equal(expected, localSettings.FolderPath);
        }

        private static List<string> GetClusterTransactionResults(RavenServer server)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return server.ServerStore.Engine.LogHistory.GetHistoryLogs(context)
                    .Where(x => (string)x["Type"] == nameof(ClusterTransactionCommand))
                    .Select(x => (string)x["Result"])
                    .ToList();
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
