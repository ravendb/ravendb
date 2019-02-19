using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Queries
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class NotModifiedQueryResults : RavenTestBase
    {
        [Fact]
        public async Task Returns_correct_results_from_cache_if_server_response_was_not_modified()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Arek", Age = 25 }, "users/1");
                    await session.StoreAsync(new User { Name = "Jan", Age = 27 }, "users/2");
                    await session.StoreAsync(new User { Name = "Arek", Age = 39 }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var commands = store.Commands())
                {
                    var users = commands.Query(new IndexQuery
                    {
                        Query = "FROM Users WHERE Name = 'Arek'",
                        WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                    });

                    Assert.Equal(2, users.Results.Length);

                    users = commands.Query(new IndexQuery
                    {
                        Query = "FROM Users WHERE Name = 'Arek'",
                        WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                    });

                    Assert.Equal(-1, users.DurationInMs); // taken from cache
                    Assert.Equal(2, users.Results.Length);
                }
            }
        }
    }
}