using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
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
        public async Task CanGetDatabaseRecordInDebugPackageFromUnsecuredServerWithoutClientCert()
        {
            var databaseName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options()
                   {
                       ModifyDatabaseName = s => databaseName
                   }))
            {
                var requestExecutor = store.GetRequestExecutor(store.Database);
                await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/admin/debug/info-package");
                using var archive = new ZipArchive(response);

                var allDatabaseEntries = DebugInfoPackageUtils.Routes.Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases)
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, store.Database)).ToList();
                allDatabaseEntries.Add($"{store.Database}/database-record.json");
                var allServerEntries = DebugInfoPackageUtils.Routes.Where(route => route.TypeOfRoute == RouteInformation.RouteType.None)
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, ServerWideDebugInfoPackageHandler._serverWidePrefix)).ToArray();
                var allExistingRouteEntries = allDatabaseEntries.Concat(allServerEntries).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(allExistingRouteEntries, archive);
            }
        }

        [Fact]
        public async Task GetOnlyOperatorAccessDebugPackageInfoFromSecuredServer()
        {
            var databaseName = GetDatabaseName();
            var certs = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.ReadWrite }, SecurityClearance.Operator);

            using (var store = GetDocumentStore(new Options() {ClientCertificate = userCert, AdminCertificate = adminCert, ModifyDatabaseName = _ => databaseName}))
            {
                var requestExecutor = store.GetRequestExecutor(databaseName);
                await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/admin/debug/info-package");
                using var archive = new ZipArchive(response);

                var databaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && OperatorAccess(route))
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, store.Database)).ToHashSet();
                databaseEntries.Add($"{store.Database}/database-record.json");
                var serverEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.None && OperatorAccess(route))
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, ServerWideDebugInfoPackageHandler._serverWidePrefix)).ToArray();
                var routeEntries = databaseEntries.Concat(serverEntries).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(routeEntries, archive);
            }
        }

        [Fact]
        public async Task GetNonAdminDebugInfoFromDatabaseDebugPackageHandler()
        {
            var databaseName = GetDatabaseName();
            var certs = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() {[databaseName] = DatabaseAccess.ReadWrite}, SecurityClearance.ValidUser);

            using (var store = GetDocumentStore(new Options()
                   {
                       ClientCertificate = userCert, 
                       AdminCertificate = adminCert, 
                       ModifyDatabaseName = _ => databaseName
                   }))
            {
                var requestExecutor = store.GetRequestExecutor(databaseName);
                await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/databases/{store.Database}/debug/info-package");
                using var archive = new ZipArchive(response);

                var nonAdminDatabaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && ReadWriteAccess(route))
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, null)).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(nonAdminDatabaseEntries, archive);
            }
        }

        [Fact]
        public async Task GetAllDebugInfoFromDatabaseDebugPackageHandlerWhenAdminDBAccess()
        {
            var databaseName = GetDatabaseName();
            var certs = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = RegisterClientCertificate(certs.ServerCertificate.Value, certs.ClientCertificate2.Value,
                new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser);

            using (var store = GetDocumentStore(new Options()
                   {
                       ClientCertificate = userCert,
                       AdminCertificate = adminCert,
                       ModifyDatabaseName = _ => databaseName
                   }))
            {
                var requestExecutor = store.GetRequestExecutor(databaseName);
                await using var response = await requestExecutor.HttpClient.GetStreamAsync($"{store.Urls.First()}/databases/{store.Database}/debug/info-package");
                using var archive = new ZipArchive(response);

                var nonAdminDatabaseEntries = DebugInfoPackageUtils.Routes
                    .Where(route => route.TypeOfRoute == RouteInformation.RouteType.Databases && DatabaseAdminAccess(route))
                    .Select(route => DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, null)).ToHashSet();

                AssertArchiveContainsAllEntriesAndOnlyThem(nonAdminDatabaseEntries, archive);
            }
        }

        private void AssertArchiveContainsAllEntriesAndOnlyThem(HashSet<string> debugEntries, ZipArchive archive)
        {
            var archiveEntries = archive.Entries.Select(entry => entry.FullName)
                .Where(e => e.Contains(".txt") == false  && (e.LastIndexOf(".error") == -1 || e.LastIndexOf(".error") != e.Length - 6)).ToHashSet();
            foreach (var e in debugEntries)
                Assert.True(archiveEntries.Contains(e), $"{e} is missing from the debug package");
            foreach (var e in archiveEntries)
                Assert.True(debugEntries.Contains(e), $"{e} should not be in the debug package");
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

