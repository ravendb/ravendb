using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents
{
    public class ModifyExistingDocument : RavenTestBase
    {
        public ModifyExistingDocument(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldThrowIfChangeCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("users/1", null,
                        new { Email = "support@ravendb.net" },
                        new Dictionary<string, object>
                        {
                            {"@collection", "Users"}
                        });

                    var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        await commands.PutAsync("users/1", null,
                            new { Email = "support@hibernatingrhinos.com" },
                            new Dictionary<string, object>
                            {
                                {"@collection", "UserAddresses"}
                            });
                    });

                    Assert.Contains("Changing 'users/1' from 'Users' to 'UserAddresses' via update is not supported. Delete it and recreate the document 'users/1'.", exception.Message);
                }
            }
        }
    }
}
