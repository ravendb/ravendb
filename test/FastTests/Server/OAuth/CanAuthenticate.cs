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
                ["db/CanGetTokenFromServer"] = AccessModes.Admin
            }
        };

        //TODO: Adi - test using only client api

        [Fact]
        public async Task CanStoreAndDeleteApiKeys()
        {
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
            using (var store = await GetDocumentStore())
            {
                StoreSampleDoc(store, "test/1");
                var securedAuthenticator = new SecuredAuthenticator();
                HttpResponseMessage result;

                // Should get PreconditionFailed on Get without token
                using (Server.Configuration.Server.SetAccessMode(AnonymousUserAccessModeValues.None))
                {
                    var client = new HttpClient();
                    result = await client.GetAsync($"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");
                    Assert.Equal(HttpStatusCode.PreconditionFailed, result.StatusCode);

                    // Should throw on DoOAuthRequestAsync with unknown apiKey
                    var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                        await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret"));
                    Assert.Contains("Could not find api key: super", exception.Message);
                }

                using (Server.Configuration.Server.SetAccessMode(AnonymousUserAccessModeValues.Admin))
                {
                    // Admin should be able to save apiKey
                    store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                    var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");
                    Assert.NotNull(doc);
                }
                using (Server.Configuration.Server.SetAccessMode(AnonymousUserAccessModeValues.None))
                {
                    // Should get token
                    var oauth =
                        await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");
                    Assert.NotNull(securedAuthenticator.CurrentToken);
                    Assert.NotEqual("", securedAuthenticator.CurrentToken);

                    // Verify successfull get with valid token
                    var authenticatedClient = new HttpClient();
                    oauth(authenticatedClient);
                    result =
                        await
                            authenticatedClient.GetAsync(
                                $"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");
                    Assert.Equal(HttpStatusCode.OK, result.StatusCode);
                }
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