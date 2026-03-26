using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23916(ITestOutputHelper output) : RavenTestBase(output)
{
    private readonly string _exceptionMessage = Raven.Server.Documents.Handlers.Processors.Queries.AbstractQueriesHandlerProcessor<AbstractDatabaseRequestHandler<JsonOperationContext>, JsonOperationContext>.CannotUseFilterClauseInPatchOrDeleteByQueryOperationExceptionMessage;

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task DeleteByCollectionQueryWithFilterClauseWillThrow(Options options)
    {
        using var store = GetDocumentStoreWithDocuments(options);
        var deleteByQuery = new DeleteByQueryOperation($"from Orders filter Company = 'RandomValue'");
        var exception = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(deleteByQuery));
        var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
        Assert.Contains(_exceptionMessage, innerException.Message);

        using (var session = store.OpenAsyncSession())
        {
            var count = await session.Query<Orders.Order>().CountAsync();
            Assert.Equal(3, count);
        }
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task PatchByCollectionQueryWithFilterClauseWillThrow(Options options)
    {
        using var store = GetDocumentStoreWithDocuments(options);
        var patchByQuery = new PatchByQueryOperation($"from Orders filter Company = 'RandomValue' update {{this.Value++;}}");
        var exception = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(patchByQuery));
        var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
        Assert.Contains(_exceptionMessage, innerException.Message);

        await AssertNoChanges(store);
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task DeleteByIndexQueryWithFilterClauseWillThrow(Options options)
    {
        using var store = GetDocumentStoreWithDocuments(options, createAutoIndex: true);
        var deleteByQuery = new DeleteByQueryOperation($"from Orders where startsWith(Company, 't') filter Company = 'RandomValue'");
        var exception = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(deleteByQuery));
        var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
        Assert.Contains(_exceptionMessage, innerException.Message);

        await AssertNoChanges(store);
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task PatchByIndexQueryWithFilterClauseWillThrow(Options options = null)
    {
        using var store = GetDocumentStoreWithDocuments(options, createAutoIndex: true);
        var patchByQuery = new PatchByQueryOperation($"from Orders where startsWith(Company, 't') filter Company = 'RandomValue' update {{this.Value++;}}");
        var exception = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(patchByQuery));
        var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
        Assert.Contains(_exceptionMessage, innerException.Message);

        await AssertNoChanges(store);
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task TestPatchByIndexQueryWithFilterClauseWillThrow(Options options = null)
    {
        using var store = GetDocumentStoreWithDocuments(options, createAutoIndex: true);
        using (var commands = store.Commands())
        {
            var patchByQueryTestCommand = new PatchByQueryTestCommand(store.Conventions, "id",
                new IndexQueryServerSide($"from Orders where startsWith(Company, 't') filter Company = 'RandomValue' update {{this.Value++;}}"
                    , queryType: QueryType.Update));
            var exception = await Assert.ThrowsAsync<RavenException>(() => commands.ExecuteAsync(patchByQueryTestCommand));
            var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains(_exceptionMessage, innerException.Message);
        }

        await AssertNoChanges(store);
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task TestPatchByCollectionQueryWithFilterClauseWillThrow(Options options = null)
    {
        using var store = GetDocumentStoreWithDocuments(options, createAutoIndex: false);

        using (var commands = store.Commands())
        {
            var patchByQueryTestCommand = new PatchByQueryTestCommand(store.Conventions, "id",
                new IndexQueryServerSide($"from Orders filter Company = 'RandomValue' update {{this.Value++;}}"
                    , queryType: QueryType.Update));
            var exception = await Assert.ThrowsAsync<RavenException>(() => commands.ExecuteAsync(patchByQueryTestCommand));
            var innerException = Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains(_exceptionMessage, innerException.Message);
        }

        await AssertNoChanges(store);
    }

    private async ValueTask AssertNoChanges(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            var count = await session.Query<Orders.Order>()
                .CountAsync();
            Assert.Equal(3, count);

            var notUpdated = await session.Advanced.AsyncDocumentQuery<Orders.Order>()
                .WaitForNonStaleResults()
                .WhereExists("Value")
                .ToListAsync();

            Assert.Empty(notUpdated);
        }
    }

    private IDocumentStore GetDocumentStoreWithDocuments(Options options = null, bool createAutoIndex = true)
    {
        var store = GetDocumentStore(options: options);
        using (var session = store.OpenSession())
        {
            session.Store(new Orders.Order() { Company = "test0" });
            session.Store(new Orders.Order() { Company = "test1" });
            session.Store(new Orders.Order() { Company = "test2" });
            session.SaveChanges();

            if (createAutoIndex)
            {
                var _ = session.Query<Orders.Order>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Company.StartsWith("test"))
                    .ToList();
            }
        }

        return store;
    }
}
