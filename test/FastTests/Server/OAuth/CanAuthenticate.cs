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
using Raven.Client.Http;
using Raven.Client.Http.OAuth;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Config.Attributes;
using Xunit;

namespace FastTests.Server.OAuth
{
    public class CanAuthenticate : RavenTestBase
    {
        private readonly ApiKeyDefinition _apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret",
            ResourcesAccessMode =
            {
                ["db/CanGetDocWithValidToken"] = AccessModes.ReadWrite,
                ["db/CanGetTokenFromServer"] = AccessModes.Admin
            }
        };

        [Fact]
        public void CanGetDocWithValidToken()
        {
            DoNotReuseServer();
            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;

            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                _apiKey.ResourcesAccessMode[store.Database] = AccessModes.ReadWrite;

                store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

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

            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            using (var store = GetDocumentStore(apiKey: "super/" + "bad secret"))
            {
                store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                var exception = Assert.Throws<AuthenticationException>(() => StoreSampleDoc(store, "test/1"));
                Assert.Contains("Unable to authenticate api key", exception.Message);
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            }
        }

        [Fact]
        public void CanStoreAndDeleteApiKeys()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);

                _apiKey.Enabled = false;
                store.Admin.Server.Send(new PutApiKeyOperation("duper", _apiKey));
                store.Admin.Server.Send(new PutApiKeyOperation("shlumper", _apiKey));
                store.Admin.Server.Send(new DeleteApiKeyOperation("shlumper"));

                var apiKeys = store.Admin.Server.Send(new GetApiKeysOperation(0, 1024)).ToList();
                Assert.Equal(2, apiKeys.Count);
                Assert.Equal("duper", apiKeys[0].UserName);
                Assert.False(apiKeys[0].Enabled);
                Assert.Equal("super", apiKeys[1].UserName);
                Assert.True(apiKeys[1].Enabled);
            }
        }

        [Fact]
        public async Task CanGetTokenFromServer()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                StoreSampleDoc(store, "test/1");

                // Should get PreconditionFailed on Get without token
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;
                var client = new HttpClient();

                var baseUrl = $"{store.Urls.First()}/databases/{store.Database}";

                var result = await client.GetAsync(baseUrl + "/docs?id=test/1");
                Assert.Equal(HttpStatusCode.PreconditionFailed, result.StatusCode);

                // Should throw on DoOAuthRequestAsync with unknown apiKey
                using (var commands = store.Commands())
                {
                    var apiKeyAuthenticator = new ApiKeyAuthenticator();
                    var exception = await Assert.ThrowsAsync<AuthenticationException>(async () => await apiKeyAuthenticator.GetAuthenticationTokenAsync("super/secret", store.Urls.First(), commands.Context));
                    Assert.Contains("Could not find api key: super", exception.Message);
                }

                // Admin should be able to save apiKey
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                _apiKey.ResourcesAccessMode[store.Database] = AccessModes.ReadWrite;
                store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);

                // Should get token
                string token;
                using (var commands = store.Commands())
                {
                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;
                    var apiKeyAuthenticator = new ApiKeyAuthenticator();
                    token = await apiKeyAuthenticator.GetAuthenticationTokenAsync("super/secret", store.Urls.First(), commands.Context);
                    Assert.NotNull(token);
                    Assert.NotEqual(string.Empty, token);
                }

                // Verify successfull get with valid token
                var authenticatedClient = new HttpClient();
                authenticatedClient.DefaultRequestHeaders.TryAddWithoutValidation("Raven-Authorization", token);
                result = await authenticatedClient.GetAsync(baseUrl + "/docs?id=test/1");
                Assert.Equal(HttpStatusCode.OK, result.StatusCode);
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            }
        }

        [Fact]
        public void ThrowOnForbiddenRequest()
        {
            DoNotReuseServer();

            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            using (var store = GetDocumentStore(apiKey: "super/" + "secret"))
            {
                store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

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
    }
}