using System.Collections.Generic;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBCL_772 : ClusterTestBase
    {
        public RDBCL_772(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldStoreLastAuthorizedNonClusterAdminRequestTime()
        {
            string dbName = GetDatabaseName();

            var customSettings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            };

            var certificates = Certificates.SetupServerAuthentication(customSettings: customSettings);
            var adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());

                    session.SaveChanges();
                }

                Assert.NotNull(Server.Statistics.LastAuthorizedNonClusterAdminRequestTime);
            }

            var result = DisposeServerAndWaitForFinishOfDisposal(Server);

            DoNotReuseServer(customSettings);

            var serverAfterRestart = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = customSettings
            });

            Assert.NotNull(serverAfterRestart.Statistics.LastAuthorizedNonClusterAdminRequestTime);
        }
    }
}
