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
        [NonLinuxFact]
        public async Task CanGetTokenFromServer()
        {
            using (var store = await GetDocumentStore())
            {
                var apiKey = new ApiKeyDefinition
                {
                    Enabled = true,
                    Secret = "secret",
                    ResourcesAccessMode =
                    {
                        ["testDbName"] =AccessModes.Admin
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new
                    {
                        Name = "test oauth"
                    }, 
                    "test/1");
                    session.SaveChanges();
                }

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                var client = new HttpClient();
                var result = await client.GetAsync($"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");

                Assert.Equal(HttpStatusCode.PreconditionFailed, result.StatusCode);

                var securedAuthenticator = new SecuredAuthenticator();
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret"));

                Assert.Contains("Could not find api key: super", exception.Message);


                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;

                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");

                Assert.NotNull(doc);

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;


                var oauth = await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");

                Assert.NotNull(securedAuthenticator.CurrentToken);
                Assert.NotEqual("", securedAuthenticator.CurrentToken);

                var authenticatedClient = new HttpClient();
                oauth(authenticatedClient);

                result = await client.GetAsync($"{store.Url}/databases/{store.DefaultDatabase}/document?id=test/1");

                Assert.Equal(HttpStatusCode.OK, result.StatusCode);
            }
        }
    }
}