using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions;
using Xunit;

namespace FastTests.Server.Documents
{
    public class ModifyExistingDocument : RavenTestBase
    {
        [Fact]
        public async Task ShouldThrowIfChangeCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("users/1", null,
                        new { Email = "support@ravendb.net" },
                        new Dictionary<string, StringValues>
                        {
                            {"@collection", "Users"}
                        });

                    var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        await commands.PutAsync("users/1", null,
                            new { Email = "support@hibernatingrhinos.com" },
                            new Dictionary<string, StringValues>
                            {
                                {"@collection", "UserAddresses"}
                            });
                    });

                    Assert.Contains("Changing 'users/1' from 'Users' to 'UserAddresses' via update is not supported." + Environment.NewLine
                                    + "Delete it and recreate the document users/1.", exception.Message);
                }
            }
        }
    }
}