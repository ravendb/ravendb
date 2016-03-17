// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.OAuth;
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
                var securedAuthenticator = new SecuredAuthenticator();
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret"));
                Assert.Contains("Could not find api key: super", exception.Message);

                var apiKey = new ApiKeyDefinition
                {
                    Enabled = true,
                    Secret = "secret",
                    ResourcesAccessMode =
                    {
                        ["testDbName"] =AccessModes.Admin
                    }
                };


                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");

                Assert.NotNull(doc);

                await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");

                Assert.NotNull(securedAuthenticator.CurrentToken);
                Assert.NotEqual("", securedAuthenticator.CurrentToken);
            }
        }
    }
}