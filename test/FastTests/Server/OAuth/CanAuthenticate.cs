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
                var doAuthTask = securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");

                var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await doAuthTask);
                Assert.Contains("Could not find document apikeys/super", exception.Message);

                var apiKey = new ApiKeyDataRequest
                {
                    Enabled = true,
                    Secret = "secret",
                };
                apiKey.ResourcesAccessMode.Add("testDbName", AccessModes.Admin);


                store.DatabaseCommands.GlobalAdmin.PutApiKey("super", apiKey);
                var doc = store.DatabaseCommands.GlobalAdmin.GetApiKey("super");

                Assert.NotNull(doc);

                securedAuthenticator = new SecuredAuthenticator();
                doAuthTask = securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");
                await doAuthTask;

                Assert.NotNull(securedAuthenticator.CurrentToken);
                Assert.NotEqual("", securedAuthenticator.CurrentToken);
            }
        }
    }
}