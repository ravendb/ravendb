using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Commands;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24118 : RavenTestBase
    {
        public RavenDB_24118(ITestOutputHelper output) : base(output)
        {
        }
        private const string RL_COMM = "RAVEN_LICENSE_COMMUNITY";

        [RavenFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_Revision()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await DisableRevisionCompression(Server, store);
                await PutLicense(Server, RL_COMM);

                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration)));
                Assert.Equal(LimitType.RevisionsConfiguration, exception.LimitType);

            }
        }

        [RavenFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Change_license()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {

                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await PutLicense(Server, RL_COMM));
                Assert.Equal(LimitType.RevisionsConfiguration, exception.LimitType);
            }
        }

        private static async Task PutLicense(RavenServer leader, string licenseType)
        {
            var license = Environment.GetEnvironmentVariable(licenseType);
            LicenseHelper.TryDeserializeLicense(license, out License li);

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
