using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBCL_772 : RavenTestBase
    {
        [Fact]
        public void ShouldStoreLastAuthorizedNonClusterAdminRequestTime()
        {
            string dbName = GetDatabaseName();

            var dataPath = NewDataPath();

            var customSettings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataPath
            };

            var serverCertPath = SetupServerAuthentication(customSettings: customSettings);
            var adminCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var clientCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
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
                Server.Router._lastRequestTimeUpdated = DateTime.MinValue;

                using (var session = store.OpenSession())
                {
                    session.Store(new User());

                    session.SaveChanges();
                }

                Assert.NotNull(Server.Statistics.LastAuthorizedNonClusterAdminRequestTime);
            }

            Server.Dispose();

            DoNotReuseServer(customSettings);

            var serverPath = Server.Configuration.Core.DataDirectory.FullPath;
            var nodePath = serverPath.Split('/').Last();

            var serverAfterRestart = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, PartialPath = nodePath, CustomSettings = customSettings });

            Assert.NotNull(serverAfterRestart.Statistics.LastAuthorizedNonClusterAdminRequestTime);
        }
    }
}
