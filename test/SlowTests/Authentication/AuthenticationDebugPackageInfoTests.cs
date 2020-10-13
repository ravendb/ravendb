using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Certificates;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public class AuthenticationDebugPackageTests : RavenTestBase
    {
        public AuthenticationDebugPackageTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenHasOperatorPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json", "storage.all-environments.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
            };
            
            var dbName = GetDatabaseName();
            
            await AssertDatabaseDebugInfoEntries(dbName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator, shouldContain);
        }
        
        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenHasClusterAdminPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json", "storage.all-environments.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
            };
            
            var dbName = GetDatabaseName();
            
            await AssertDatabaseDebugInfoEntries(dbName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, shouldContain);
        }
        
        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenDatabaseAdminPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json", "storage.all-environments.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
            };
            
            var dbName = GetDatabaseName();
            var databaseAccesses = new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.Admin };
            
            await AssertDatabaseDebugInfoEntries(dbName, databaseAccesses, SecurityClearance.ValidUser, shouldContain);
        }
        
        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenValidUserWithOnlyReadWritePermissions_ShouldNotContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json", "storage.all-environments.report.json",
                "etl.stats.json", "etl.progress.json",
            };
            
            var dbName = GetDatabaseName();
            var databaseAccesses = new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite };
            
            await AssertDatabaseDebugInfoEntries(dbName, databaseAccesses, SecurityClearance.ValidUser, shouldContain);
        }

        private async Task AssertDatabaseDebugInfoEntries(string dbName, Dictionary<string, DatabaseAccess> databaseAccesses,
            SecurityClearance securityClearance, string[] shouldContain)
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var userCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, databaseAccesses, securityClearance);

            using var store = GetDocumentStore(new Options {AdminCertificate = adminCert, ClientCertificate = userCert, ModifyDatabaseName = s => dbName});
            var requestExecutor = store.GetRequestExecutor(store.Database);
            await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/databases/{dbName}/debug/info-package");
            using var archive = new ZipArchive(response);
            var entries = archive.Entries.Select(e => e.Name).ToArray();

            var shouldContainButNot = shouldContain.Except(entries).ToArray();
            var shouldNotContainButDo = entries.Except(shouldContain).ToArray();
            Assert.True(shouldContainButNot.Any() == false && shouldNotContainButDo.Any() == false, 
                $"Should contain \"{string.Join(", ", shouldContainButNot)}\", Should not contain \"{string.Join(", ", shouldNotContainButDo)}\"");
        }
    }
}
