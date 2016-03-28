// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Client.Data;
using Raven.Client.OAuth;
using Raven.Server.Config.Attributes;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Server.OAuth
{
    public class CanAuthenticate : RavenTestBase
    {
        private ApiKeyDefinition apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret",
            ResourcesAccessMode =
            {
                ["db/CanGetDocWithValidToken"] = AccessModes.ReadWrite,
                ["db/CanGetTokenFromServer"] = AccessModes.Admin
            }
        };

        [NonLinuxFact]
        public async Task CanGetDocWithValidToken()
        {
            DoNotReuseServer();

            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            using (var store = await GetDocumentStore(apiKey: "super /" + apiKey.Secret))
            {
                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");
                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                StoreSampleDoc(store, "test/1");

                dynamic test1doc;
                using (var session = store.OpenSession())
                    test1doc = session.Load<dynamic>("test/1");

                Assert.NotNull(test1doc);
            }
        }

        [NonLinuxFact]
        public async Task CanNotGetDocWithInalidToken()
        {
            DoNotReuseServer();

            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            using (var store = await GetDocumentStore(apiKey: "super/" + "bad secret"))
            {
                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");
                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                var exception = Assert.Throws<InvalidApiKeyException>(() => StoreSampleDoc(store, "test/1"));
                Assert.Contains("Unable to authenticate api key", exception.Message);
            }
        }

        [Fact]
        public async Task CanStoreAndDeleteApiKeys()
        {
            DoNotReuseServer();

            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");
                Assert.NotNull(doc);

                apiKey.Enabled = false;
                store.DatabaseCommands.GlobalAdmin.PutApiKey("duper", apiKey);
                store.DatabaseCommands.GlobalAdmin.PutApiKey("shlumper", apiKey);
                store.DatabaseCommands.GlobalAdmin.DeleteApiKey("shlumper");

                var apiKeys = store.DatabaseCommands.GlobalAdmin.GetAllApiKeys().ToList();
                Assert.Equal(2, apiKeys.Count);
                Assert.Equal("duper", apiKeys[0].UserName);
                Assert.False(apiKeys[0].Enabled);
                Assert.Equal("super", apiKeys[1].UserName);
                Assert.True(apiKeys[1].Enabled);
            }
        }

        [NonLinuxFact]
        public async Task CanGetTokenFromServer()
        {
            DoNotReuseServer();
            using (var store = await GetDocumentStore())
            {
                StoreSampleDoc(store, "test/1");

                // Should get PreconditionFailed on Get without token
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;
                var client = new HttpClient();
                var result = await client.GetAsync($"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");
                Assert.Equal(HttpStatusCode.PreconditionFailed, result.StatusCode);

                // Should throw on DoOAuthRequestAsync with unknown apiKey
                var securedAuthenticator = new SecuredAuthenticator();
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret"));
                Assert.Contains("Could not find api key: super", exception.Message);

                // Admin should be able to save apiKey
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");
                Assert.NotNull(doc);

                // Should get token
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;
                var oauth = await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");
                Assert.NotNull(securedAuthenticator.CurrentToken);
                Assert.NotEqual("", securedAuthenticator.CurrentToken);

                // Verify successfull get with valid token
                var authenticatedClient = new HttpClient();
                oauth(authenticatedClient);
                result = await authenticatedClient.GetAsync($"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");
                Assert.Equal(HttpStatusCode.OK, result.StatusCode);
            }
        }

        private static void StoreSampleDoc(Raven.Client.Document.DocumentStore store, string docName)
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