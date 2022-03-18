using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17577 : RavenTestBase
{
    public RavenDB_17577(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void WillRejectJsIndexDeploymentAsValidUserIfItIsRestrictedToAdminsOnly()
    {
        var certificates = Certificates.SetupServerAuthentication(customSettings: new Dictionary<string, string>
        {
            { RavenConfiguration.GetKey(x => x.Indexing.RequireAdminToDeployJavaScriptIndexes), "true"}
        });

        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
        {
            [dbName] = DatabaseAccess.ReadWrite
        }, SecurityClearance.ValidUser);

        using (var store = GetDocumentStore(new Options
               {
                   AdminCertificate = adminCert, 
                   ClientCertificate = userCert, 
                   ModifyDatabaseName = s => dbName,
               }))
        {
            var index = new UsersByName();

            Assert.Throws<AuthorizationException>(() => store.ExecuteIndex(index));

            using (var storeWithAdminCert = new DocumentStore
                   {
                       Urls = store.Urls,
                       Certificate = adminCert,
                       Database = store.Database
                   }.Initialize())
            {
                storeWithAdminCert.ExecuteIndex(index);
            }
        }
    }

    private class UsersByName : AbstractJavaScriptIndexCreationTask
    {
        public UsersByName()
        {
            Maps = new HashSet<string>
            {
                @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
            };
        }
    }
}
