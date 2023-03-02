using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Certificates;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using HttpMethod = System.Net.Http.HttpMethod;

namespace SlowTests.Issues;

public class RavenDB_19101: RavenTestBase
{
    public RavenDB_19101(ITestOutputHelper output) : base(output)
    {
    }
    
    private class GetCertificateFromIndexCommand : RavenCommand<object>
    {
        public GetCertificateFromIndexCommand()
        {

        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/indexes/history?name=DummyIndex";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
     }

    [Fact]
    public void CheckIfCertificateNameIsReturned()
    {
        var certificates = Certificates.SetupServerAuthentication();
        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

        DoNotReuseServer();
        
        using (var store = GetDocumentStore(new Options { AdminCertificate = adminCert, ClientCertificate = adminCert, ModifyDatabaseName = s => dbName }))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User(){Name = "CoolName"});
                
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new GetCertificateFromIndexCommand();
                commands.Execute(cmd);
                var res = cmd.Result;

                var blit = res as BlittableJsonReaderObject;

                blit.TryGet("History", out BlittableJsonReaderArray history);

                var firstHistory = history.FirstOrDefault() as BlittableJsonReaderObject;

                firstHistory.TryGet("CertificateThumbprint", out string certificateThumbprint);

                Assert.Equal(store.Certificate?.Thumbprint, certificateThumbprint);
            }
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<User>
    {
        public DummyIndex()
        {
            Map = users => from user in users
                select new {Name = user.Name};
        }
    }
}
