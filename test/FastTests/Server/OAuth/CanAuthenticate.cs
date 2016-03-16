// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Data;
using Raven.Client.OAuth;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.Abstractions.Data;
using Voron;

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