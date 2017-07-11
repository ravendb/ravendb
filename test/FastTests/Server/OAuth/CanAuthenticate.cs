// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.Certificates;
using Xunit;

namespace FastTests.Server.OAuth
{
    /*public class CanAuthenticate : RavenTestBase
    {
        private readonly ApiKeyDefinition _apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret"
        };

        [Fact]
        public void CanGetDocWithValidToken()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;

            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                _apiKey.ResourcesAccessMode[store.Database] = AccessMode.ReadWrite;

                store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                Assert.NotNull(doc);

                Server.Configuration.Security.AuthenticationEnabled = true;

                StoreSampleDoc(store, "test/1");

                dynamic test1Doc;
                using (var session = store.OpenSession())
                    test1Doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1Doc);
            }
        }

        [Fact]
        public void CanNotGetDocWithInvalidToken()
        {
            DoNotReuseServer();

           Server.Configuration.Security.AuthenticationEnabled = false;
            using (var store = GetDocumentStore(apiKey: "super/" + "bad secret"))
            {
                store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                Assert.NotNull(doc);

                Server.Configuration.Security.AuthenticationEnabled = true;

                var exception = Assert.Throws<AuthenticationException>(() => StoreSampleDoc(store, "test/1"));
                Assert.Contains("Unable to authenticate api key", exception.Message);
                Server.Configuration.Security.AuthenticationEnabled = false;
            }
        }

        [Fact]
        public void CanStoreAndDeleteApiKeys()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                Assert.NotNull(doc);

                _apiKey.Enabled = false;
                store.Admin.Server.Send(new PutCertificateOperation("duper", _apiKey));
                store.Admin.Server.Send(new PutCertificateOperation("shlumper", _apiKey));
                store.Admin.Server.Send(new DeleteCertificateOperation("shlumper"));

                var apiKeys = store.Admin.Server.Send(new GetCertificatesOperation(0, 1024)).ToList();
                Assert.Equal(2, apiKeys.Count);
                Assert.Equal("duper", apiKeys[0].UserName);
                Assert.False(apiKeys[0].Enabled);
                Assert.Equal("super", apiKeys[1].UserName);
                Assert.True(apiKeys[1].Enabled);
            }
        }

        [Fact]
        public void ThrowOnForbiddenRequest()
        {
            DoNotReuseServer();

            Server.Configuration.Security.AuthenticationEnabled = false;
            using (var store = GetDocumentStore(apiKey: "super/" + "secret"))
            {
                store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                Assert.NotNull(doc);

                 Server.Configuration.Security.AuthenticationEnabled = true;

                Assert.Throws<AuthorizationException>(() => StoreSampleDoc(store, "test/1"));
            }
        }

        private static void StoreSampleDoc(DocumentStore store, string docName)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new
                {
                    Name = "test oauth"
                },
                docName);
                session.SaveChanges();
            }
        }
    }*/
}