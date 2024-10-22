using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public class AuthenticationDebugPackageTests : RavenTestBase
    {
        private readonly string[] _routesToSkip = new string[] { "/admin/debug/threads/stack-trace" };

        public AuthenticationDebugPackageTests(ITestOutputHelper output) : base(output)
        {
        }

        [LinuxFact]
        public async Task WriteMeminfoAsTextFileInDebugPackage_RavenDB_17427()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                Server.ForTestingPurposesOnly().DebugPackage.RoutesToSkip = _routesToSkip;
                var requestExecutor = store.GetRequestExecutor(store.Database);
                await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/admin/debug/info-package");
                using var archive = new ZipArchive(response);

                var meminfoEntries = archive.Entries.Where(entry => entry.Name.Contains("proc.meminfo")).OrderBy(e => e.Name).ToList();

                //if meminfo writing errored, might have both empty meminfo.txt file and meminfo.error file.
                //check both don't contain the issue's error and aren't empty
                foreach (var meminfo in meminfoEntries)
                {
                    using (var stream = meminfo.Open())
                    using (var reader = new StreamReader(stream))
                    {
                        var contents = await reader.ReadToEndAsync();
                        Assert.DoesNotContain("System.IO.InvalidDataException: Cannot have a", contents);
                        Assert.Contains("MemFree", contents); //making sure some info exists in the file
                    }
                }
            }
        }

        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenHasOperatorPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "indexes.metadata.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
                "admin.tombstones.state.json", "indexes.performance.json"
            };

            var dbName = GetDatabaseName();

            await AssertDatabaseDebugInfoEntries(dbName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator, shouldContain);
        }

        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenHasClusterAdminPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "indexes.metadata.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
                "admin.tombstones.state.json", "indexes.performance.json"
            };

            var dbName = GetDatabaseName();

            await AssertDatabaseDebugInfoEntries(dbName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, shouldContain);
        }

        [Fact]
        public async Task DatabaseDebugInfoPackage_WhenDatabaseAdminPermissions_ShouldContainSettingsJson()
        {
            var shouldContain = new[]
            {
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "indexes.metadata.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json",
                "admin.txinfo.json", "admin.cluster.txinfo.json", "admin.configuration.settings.json", "etl.stats.json", "etl.progress.json",
                "admin.tombstones.state.json", "indexes.performance.json"
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
                "tasks.json", "indexes.json", "indexes.stats.json", "indexes.errors.json", "indexes.metadata.json", "io-metrics.json", "perf-metrics.json",
                "replication.outgoing-failures.json", "replication.incoming-last-activity-time.json", "replication.incoming-rejection-info.json",
                "replication.outgoing-reconnect-queue.json", "stats.json", "subscriptions.json", "tcp.json", "documents.huge.json", "identities.json",
                "queries.running.json", "queries.cache.list.json", "script-runners.json", "storage.report.json",
                "etl.stats.json", "etl.progress.json", "indexes.performance.json"
            };

            var dbName = GetDatabaseName();
            var databaseAccesses = new Dictionary<string, DatabaseAccess> { [dbName] = DatabaseAccess.ReadWrite };

            await AssertDatabaseDebugInfoEntries(dbName, databaseAccesses, SecurityClearance.ValidUser, shouldContain);
        }

        private async Task AssertDatabaseDebugInfoEntries(string dbName, Dictionary<string, DatabaseAccess> databaseAccesses,
            SecurityClearance securityClearance, string[] shouldContain)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, databaseAccesses, securityClearance);

            using var store = GetDocumentStore(new Options { AdminCertificate = adminCert, ClientCertificate = userCert, ModifyDatabaseName = s => dbName });
            var requestExecutor = store.GetRequestExecutor(store.Database);
            var response = await requestExecutor.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/databases/{dbName}/debug/info-package").WithConventions(store.Conventions));
            await using var stream = await response.Content.ReadAsStreamAsync();
            using var archive = new ZipArchive(stream);
            var entries = archive.Entries.Select(e => e.Name).ToArray();

            var shouldContainButNot = shouldContain.Except(entries).ToArray();
            var shouldNotContainButDo = entries.Except(shouldContain).ToArray();
            Assert.True(shouldContainButNot.Any() == false && shouldNotContainButDo.Any() == false,
                $"Should contain \"{string.Join(", ", shouldContainButNot)}\", Should not contain \"{string.Join(", ", shouldNotContainButDo)}\"");
        }

        [NightlyBuildFact]
        public async Task CanGetDatabaseRecordInDebugPackageFromUnsecuredServerWithoutClientCert()
        {
            DoNotReuseServer();
            var databaseName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDatabaseName = s => databaseName
            }))
            {
                Server.ForTestingPurposesOnly().DebugPackage.RoutesToSkip = _routesToSkip;
                var requestExecutor = store.GetRequestExecutor(store.Database);
                var response = await requestExecutor.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/admin/debug/info-package").WithConventions(store.Conventions));
                await using var stream = await response.Content.ReadAsStreamAsync();
                using var archive = new ZipArchive(stream);

                var allDatabaseEntries = DebugInfoPackageUtils.Routes.Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases)
                    .Select(route => GetFileNameWithoutExtension(route, store.Database)).ToList();
                allDatabaseEntries.Add($"{store.Database}/database-record");
                var allServerEntries = DebugInfoPackageUtils.Routes.Where(route => route.TypeOfRoute == RouteInformation.RouteType.None && _routesToSkip.Contains(route.Path) == false)
                    .Select(route => GetFileNameWithoutExtension(route, ServerWideDebugInfoPackageHandler._serverWidePrefix)).ToArray();
                var allExistingRouteEntries = allDatabaseEntries.Concat(allServerEntries).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(allExistingRouteEntries, archive);
            }
        }

        [NightlyBuildFact]
        public async Task GetOnlyOperatorAccessDebugPackageInfoFromSecuredServer()
        {
            DoNotReuseServer();
            var databaseName = GetDatabaseName();
            var certs = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.ReadWrite }, SecurityClearance.Operator);

            using (var store = GetDocumentStore(new Options() { ClientCertificate = userCert, AdminCertificate = adminCert, ModifyDatabaseName = _ => databaseName }))
            {
                Server.ForTestingPurposesOnly().DebugPackage.RoutesToSkip = _routesToSkip;
                var requestExecutor = store.GetRequestExecutor(databaseName);
                var response = await requestExecutor.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get,$"{store.Urls.First()}/admin/debug/info-package").WithConventions(store.Conventions));
                await using var stream = await response.Content.ReadAsStreamAsync();
                using var archive = new ZipArchive(stream);

                var databaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && OperatorAccess(route))
                    .Select(route => GetFileNameWithoutExtension(route, store.Database)).ToHashSet();
                databaseEntries.Add($"{store.Database}/database-record");
                var serverEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.None && OperatorAccess(route) && _routesToSkip.Contains(route.Path) == false)
                    .Select(route => GetFileNameWithoutExtension(route, ServerWideDebugInfoPackageHandler._serverWidePrefix)).ToArray();

                var routeEntries = databaseEntries.Concat(serverEntries).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(routeEntries, archive);
            }
        }

        [Fact]
        public async Task GetNonAdminDebugInfoFromDatabaseDebugPackageHandler()
        {
            DoNotReuseServer();
            var databaseName = GetDatabaseName();
            var certs = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.ReadWrite }, SecurityClearance.ValidUser);

            using (var store = GetDocumentStore(new Options()
            {
                ClientCertificate = userCert,
                AdminCertificate = adminCert,
                ModifyDatabaseName = _ => databaseName
            }))
            {
                Server.ForTestingPurposesOnly().DebugPackage.RoutesToSkip = _routesToSkip;
                var requestExecutor = store.GetRequestExecutor(databaseName);
                var response = await requestExecutor.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get,$"{store.Urls.First()}/databases/{store.Database}/debug/info-package").WithConventions(store.Conventions));
                await using var stream = await response.Content.ReadAsStreamAsync();
                using var archive = new ZipArchive(stream);

                var nonAdminDatabaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && ReadWriteAccess(route))
                    .Select(route => GetFileNameWithoutExtension(route, null)).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(nonAdminDatabaseEntries, archive);
            }
        }

        [Fact]
        public async Task GetAllDebugInfoFromDatabaseDebugPackageHandlerWhenAdminDBAccess()
        {
            DoNotReuseServer();
            var databaseName = GetDatabaseName();
            var certs = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser);

            using (var store = GetDocumentStore(new Options()
            {
                ClientCertificate = userCert,
                AdminCertificate = adminCert,
                ModifyDatabaseName = _ => databaseName
            }))
            {
                Server.ForTestingPurposesOnly().DebugPackage.RoutesToSkip = _routesToSkip;
                var requestExecutor = store.GetRequestExecutor(databaseName);
                var response = await requestExecutor.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get,$"{store.Urls.First()}/databases/{store.Database}/debug/info-package").WithConventions(store.Conventions));
                await using var stream = await response.Content.ReadAsStreamAsync();
                using var archive = new ZipArchive(stream);

                var nonAdminDatabaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && DatabaseAdminAccess(route))
                    .Select(route => GetFileNameWithoutExtension(route, null)).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(nonAdminDatabaseEntries, archive);
            }
        }

        private void AssertArchiveContainsAllEntriesAndOnlyThem(HashSet<string> debugEntries, ZipArchive archive)
        {
            var archiveEntries = archive.Entries.Select(entry => entry.FullName)
                .Where(e => e.Contains($"{DateTime.UtcNow:yyyy-MM-dd}") == false && (e.LastIndexOf(".error") == -1 || e.LastIndexOf(".error") != e.Length - 6))
                .Select(e => e.Replace(".txt", "").Replace(".json", "")).ToHashSet();
            foreach (var e in debugEntries)
                Assert.True(archiveEntries.Contains(e), $"{e} is missing from the debug package");
            foreach (var e in archiveEntries)
                Assert.True(debugEntries.Contains(e) || e.Contains("requestTimes"), $"{e} should not be in the debug package");
        }

        private string GetFileNameWithoutExtension(RouteInformation route, string prefixFolder)
        {
            return DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, prefixFolder, null);
        }

        private bool ReadWriteAccess(RouteInformation route)
        {
            return route.AuthorizationStatus == AuthorizationStatus.ValidUser ||
                   route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients;
        }

        private bool DatabaseAdminAccess(RouteInformation route)
        {
            return route.AuthorizationStatus == AuthorizationStatus.ValidUser || route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin ||
                   route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients;
        }

        private bool OperatorAccess(RouteInformation route)
        {
            return route.AuthorizationStatus == AuthorizationStatus.ValidUser || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients ||
                   route.AuthorizationStatus == AuthorizationStatus.Operator || route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin;
        }
    }
}
