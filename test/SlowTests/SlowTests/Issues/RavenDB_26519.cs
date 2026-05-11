using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_26519 : DisableParallelTestBase
    {
        public RavenDB_26519(ITestOutputHelper output) : base(output)
        {
        }

        private sealed class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Querying)]
        [RavenData("from Users", DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData("from @all_docs", DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task DeleteByQuery_Sharded_ShouldWriteAuditLine(Options options, string query)
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

            var op = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = query }));
            await op.WaitForCompletionAsync(System.TimeSpan.FromMinutes(1));

            // Sharded mode transitively verifies the OperationType fix: if the sharded delete
            // handler still reported UpdateByQuery, the verb here would render as "UPDATE".
            var line = await AuditLogTestHelper.WaitForAuditLineAsync(auditLogPath,
                l => l.Contains("DELETE") && l.Contains($"Documents matching the query: {query}"));

            Assert.NotNull(line);
        }

        [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Querying | RavenTestCategory.Patching)]
        [RavenData("from Users update { this.Patched = true }", DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData("from @all_docs update { this.Patched = true }", DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task PatchByQuery_Sharded_ShouldWriteAuditLine(Options options, string query)
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

            var op = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery { Query = query }));
            await op.WaitForCompletionAsync(System.TimeSpan.FromMinutes(1));

            var line = await AuditLogTestHelper.WaitForAuditLineAsync(auditLogPath,
                l => l.Contains("UPDATE") && l.Contains($"Documents matching the query: {query}"));

            Assert.NotNull(line);
        }
    }
}
