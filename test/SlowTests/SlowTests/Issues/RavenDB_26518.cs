using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Commands.Studio;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_26518 : DisableParallelTestBase
    {
        public RavenDB_26518(ITestOutputHelper output) : base(output)
        {
        }

        private sealed class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task StudioCollectionDelete_ShouldWriteAuditLine(Options options)
        {
            var (auditLogPath, adminCert) = AuditLogTestHelper.SetupAuditLoggedServer(this);
            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;

            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "alice" });
                session.Store(new User { Name = "bob" });
                session.SaveChanges();
            }

            await store.Operations.SendAsync(new DeleteStudioCollectionOperation(operationId: null, collectionName: "Users", excludeIds: null));

            var line = await AuditLogTestHelper.WaitForAuditLineAsync(auditLogPath,
                l => l.Contains("DELETE") && l.Contains("Documents in collection 'Users'"));

            Assert.NotNull(line);
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task StudioCollectionDelete_AllDocs_ShouldWriteAuditLine(Options options)
        {
            var (auditLogPath, adminCert) = AuditLogTestHelper.SetupAuditLoggedServer(this);
            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;

            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "alice" });
                session.SaveChanges();
            }

            await store.Operations.SendAsync(new DeleteStudioCollectionOperation(operationId: null, collectionName: "@all_docs", excludeIds: null));

            var line = await AuditLogTestHelper.WaitForAuditLineAsync(auditLogPath,
                l => l.Contains("DELETE") && l.Contains("Documents in collection '@all_docs'"));

            Assert.NotNull(line);
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task StudioCollectionDelete_WithExcludeIds_ShouldIncludeExclusionCount(Options options)
        {
            var (auditLogPath, adminCert) = AuditLogTestHelper.SetupAuditLoggedServer(this);
            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;

            using var store = GetDocumentStore(options);

            string keepId;
            using (var session = store.OpenSession())
            {
                var keep = new User { Name = "keep" };
                session.Store(new User { Name = "drop" });
                session.Store(keep);
                session.SaveChanges();
                keepId = keep.Id;
            }

            await store.Operations.SendAsync(new DeleteStudioCollectionOperation(operationId: null, collectionName: "Users", excludeIds: new List<string> { keepId }));

            var line = await AuditLogTestHelper.WaitForAuditLineAsync(auditLogPath,
                l => l.Contains("DELETE")
                     && l.Contains("Documents in collection 'Users'")
                     && l.Contains("(excluding 1 ids)"));

            Assert.NotNull(line);
        }
    }
}
