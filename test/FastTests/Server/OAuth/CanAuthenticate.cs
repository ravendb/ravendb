// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
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

                var apiKeysEnum = store.DatabaseCommands.GlobalAdmin.StreamApiKeys(100);
                var enumerateApiKeys = apiKeysEnum.GetEnumerator();
                
                // we expect *d*uper to come before *s*uper
                Assert.True(enumerateApiKeys.MoveNext());
                Assert.Equal("duper", enumerateApiKeys.Current.UserName);
                Assert.False(enumerateApiKeys.Current.Enabled);

                Assert.True(enumerateApiKeys.MoveNext());
                Assert.Equal("super", enumerateApiKeys.Current.UserName);
                Assert.True(enumerateApiKeys.Current.Enabled);

                Assert.False(enumerateApiKeys.MoveNext()); // Ensure Deletion
            }
        }

        [NonLinuxFact]
        public async Task CanGetTokenFromServer()
        {
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