using System;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Json.Linq;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Server.Documents
{
    public class ModifyExistingDocument : RavenTestBase
    {
        [Fact]
        public async Task ShouldThrowIfChangeRavenEntityName()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("users/1", null, 
                    RavenJObject.Parse("{\"Email\":\"support@ravendb.net\"}"), 
                    RavenJObject.Parse("{\"Raven-Entity-Name\":\"Users\"}"));

                var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PutAsync("users/1", null,
                        RavenJObject.Parse("{\"Email\":\"support@hibernatingrhinos.com\"}"),
                        RavenJObject.Parse("{\"Raven-Entity-Name\":\"UserAddresses\"}"));
                });
                Assert.Contains("InvalidOperationException: Changing 'users/1' from 'Users' to 'UserAddresses' via update is not supported." +Environment.NewLine
                                + "Delete the document and recreate the document users/1.", exception.Message);
            }
        }
    }
}