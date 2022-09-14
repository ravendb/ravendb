using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Queries;

public class RavenDB_18257 : RavenTestBase
{
    public RavenDB_18257(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ShouldReturnNotModified(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            for (int i = 0; i < 100; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = i % 2 == 0 ? "Arek" : "Joe" }, $"users/{i}");

                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenSession())
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var query = new IndexQuery
                {
                    Query = "FROM Users WHERE Name = 'Arek'",
                    WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                };

                var command = new QueryCommand((InMemoryDocumentSessionOperations)session, query);

                await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                var users = command.Result;

                Assert.Equal(50, users.Results.Length);

                var command2 = new QueryCommand((InMemoryDocumentSessionOperations)session, query);

                await session.Advanced.RequestExecutor.ExecuteAsync(command2, context);

                Assert.Equal(HttpStatusCode.NotModified, command2.StatusCode);
                Assert.Equal(-1, command2.Result.DurationInMs); // taken from cache

                // let's modify a single document to ensure we won't get stale results and NotModified
                var user = session.Load<User>("users/0");

                user.Name = "Foo";

                session.SaveChanges();

                var command3 = new QueryCommand((InMemoryDocumentSessionOperations)session, query);

                await session.Advanced.RequestExecutor.ExecuteAsync(command3, context);

                Assert.NotEqual(HttpStatusCode.NotModified, command3.StatusCode);

                users = command3.Result;

                Assert.Equal(49, users.Results.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ShouldNotReturnNotModifiedIfOrderByRandomApplied(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            for (int i = 0; i < 100; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = i % 2 == 0 ? "Arek" : "Joe" }, $"users/{i}");

                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenSession())
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var query = new IndexQuery
                {
                    Query = "FROM Users WHERE Name = 'Arek' order by random()",
                    WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                };

                var command = new QueryCommand((InMemoryDocumentSessionOperations)session, query);

                await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                var users = command.Result;

                Assert.Equal(50, users.Results.Length);

                var command2 = new QueryCommand((InMemoryDocumentSessionOperations)session, query);

                await session.Advanced.RequestExecutor.ExecuteAsync(command2, context);

                Assert.NotEqual	(HttpStatusCode.NotModified, command2.StatusCode);

                users = command2.Result;
                Assert.Equal(50, users.Results.Length);

            }
        }
    }
}
