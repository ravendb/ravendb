// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Abstractions.OAuth;
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
                await securedAuthenticator.DoOAuthRequestAsync(store.Url + "/oauth/api-key", "super/secret");

                Assert.NotNull(securedAuthenticator.CurrentToken);
            }
        }
    }
}