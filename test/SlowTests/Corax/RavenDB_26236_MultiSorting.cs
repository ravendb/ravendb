using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

public class RavenDB_26236_MultiSorting(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingStringViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var fieldClause = $"Name {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var orderBy = fieldFirst ? $"{fieldClause}, ToIgnore" : $"ToIgnore, {fieldClause}";
        var rql = $"{fromClause} where exists(id()) order by {orderBy}";

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingStringViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);

        IOrderedQueryable<Document> ordered;
        if (fieldFirst)
        {
            var primary = isAscending
                ? baseQuery.OrderBy(x => x.Name, nulls, OrderingType.String)
                : baseQuery.OrderByDescending(x => x.Name, nulls, OrderingType.String);
            ordered = primary.ThenBy(x => x.ToIgnore);
        }
        else
        {
            var primary = baseQuery.OrderBy(x => x.ToIgnore);
            ordered = isAscending
                ? primary.ThenBy(x => x.Name, nulls, OrderingType.String)
                : primary.ThenByDescending(x => x.Name, nulls, OrderingType.String);
        }

        var queryResults = await ordered.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingStringViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query.WhereExists(x => x.Id);

        if (fieldFirst)
        {
            query = isAscending
                ? query.OrderBy(x => x.Name, nulls, OrderingType.String)
                : query.OrderByDescending(x => x.Name, nulls, OrderingType.String);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending
                ? query.OrderBy(x => x.Name, nulls, OrderingType.String)
                : query.OrderByDescending(x => x.Name, nulls, OrderingType.String);
        }

        var queryResults = await query.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingIntViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var fieldClause = $"IntValue as long {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var orderBy = fieldFirst ? $"{fieldClause}, ToIgnore" : $"ToIgnore, {fieldClause}";
        var rql = $"{fromClause} where exists(id()) order by {orderBy}";

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingIntViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);

        IOrderedQueryable<Document> ordered;
        if (fieldFirst)
        {
            var primary = isAscending
                ? baseQuery.OrderBy(x => x.IntValue, nulls, OrderingType.Long)
                : baseQuery.OrderByDescending(x => x.IntValue, nulls, OrderingType.Long);
            ordered = primary.ThenBy(x => x.ToIgnore);
        }
        else
        {
            var primary = baseQuery.OrderBy(x => x.ToIgnore);
            ordered = isAscending
                ? primary.ThenBy(x => x.IntValue, nulls, OrderingType.Long)
                : primary.ThenByDescending(x => x.IntValue, nulls, OrderingType.Long);
        }

        var queryResults = await ordered.ToListAsync();
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingIntViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query.WhereExists(x => x.Id);

        if (fieldFirst)
        {
            query = isAscending
                ? query.OrderBy(x => x.IntValue, nulls, OrderingType.Long)
                : query.OrderByDescending(x => x.IntValue, nulls, OrderingType.Long);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending
                ? query.OrderBy(x => x.IntValue, nulls, OrderingType.Long)
                : query.OrderByDescending(x => x.IntValue, nulls, OrderingType.Long);
        }

        var queryResults = await query.ToListAsync();
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingDoubleViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var fieldClause = $"DoubleValue as double {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var orderBy = fieldFirst ? $"{fieldClause}, ToIgnore" : $"ToIgnore, {fieldClause}";
        var rql = $"{fromClause} where exists(id()) order by {orderBy}";

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingDoubleViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);

        IOrderedQueryable<Document> ordered;
        if (fieldFirst)
        {
            var primary = isAscending
                ? baseQuery.OrderBy(x => x.DoubleValue, nulls, OrderingType.Double)
                : baseQuery.OrderByDescending(x => x.DoubleValue, nulls, OrderingType.Double);
            ordered = primary.ThenBy(x => x.ToIgnore);
        }
        else
        {
            var primary = baseQuery.OrderBy(x => x.ToIgnore);
            ordered = isAscending
                ? primary.ThenBy(x => x.DoubleValue, nulls, OrderingType.Double)
                : primary.ThenByDescending(x => x.DoubleValue, nulls, OrderingType.Double);
        }

        var queryResults = await ordered.ToListAsync();
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingDoubleViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query.WhereExists(x => x.Id);

        if (fieldFirst)
        {
            query = isAscending
                ? query.OrderBy(x => x.DoubleValue, nulls, OrderingType.Double)
                : query.OrderByDescending(x => x.DoubleValue, nulls, OrderingType.Double);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending
                ? query.OrderBy(x => x.DoubleValue, nulls, OrderingType.Double)
                : query.OrderByDescending(x => x.DoubleValue, nulls, OrderingType.Double);
        }

        var queryResults = await query.ToListAsync();
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingAlphaNumericViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var fieldClause = $"Name as alphanumeric {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var orderBy = fieldFirst ? $"{fieldClause}, ToIgnore" : $"ToIgnore, {fieldClause}";
        var rql = $"{fromClause} where exists(id()) order by {orderBy}";

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingAlphaNumericViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);

        IOrderedQueryable<Document> ordered;
        if (fieldFirst)
        {
            var primary = isAscending
                ? baseQuery.OrderBy(x => x.Name, nulls, OrderingType.AlphaNumeric)
                : baseQuery.OrderByDescending(x => x.Name, nulls, OrderingType.AlphaNumeric);
            ordered = primary.ThenBy(x => x.ToIgnore);
        }
        else
        {
            var primary = baseQuery.OrderBy(x => x.ToIgnore);
            ordered = isAscending
                ? primary.ThenBy(x => x.Name, nulls, OrderingType.AlphaNumeric)
                : primary.ThenByDescending(x => x.Name, nulls, OrderingType.AlphaNumeric);
        }

        var queryResults = await ordered.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingAlphaNumericViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query.WhereExists(x => x.Id);

        if (fieldFirst)
        {
            query = isAscending
                ? query.OrderBy(x => x.Name, nulls, OrderingType.AlphaNumeric)
                : query.OrderByDescending(x => x.Name, nulls, OrderingType.AlphaNumeric);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending
                ? query.OrderBy(x => x.Name, nulls, OrderingType.AlphaNumeric)
                : query.OrderByDescending(x => x.Name, nulls, OrderingType.AlphaNumeric);
        }

        var queryResults = await query.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingSpatialViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var distance = isAutoIndex
            ? "spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0))"
            : "spatial.distance(Location, spatial.point(0,0))";
        var fieldClause = $"{distance} {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var orderBy = fieldFirst ? $"{fieldClause}, ToIgnore" : $"ToIgnore, {fieldClause}";
        var rql = $"{fromClause} where exists(id()) order by {orderBy}";

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertSpatialNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenMultiFieldSortingSpatialViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query.WhereExists(x => x.Id);

        if (fieldFirst)
        {
            if (isAutoIndex)
                query = isAscending
                    ? query.OrderByDistance(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls)
                    : query.OrderByDistanceDescending(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls);
            else
                query = isAscending
                    ? query.OrderByDistance(x => x.Location, 0, 0, nulls)
                    : query.OrderByDistanceDescending(x => x.Location, 0, 0, nulls);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            if (isAutoIndex)
                query = isAscending
                    ? query.OrderByDistance(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls)
                    : query.OrderByDistanceDescending(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls);
            else
                query = isAscending
                    ? query.OrderByDistance(x => x.Location, 0, 0, nulls)
                    : query.OrderByDistanceDescending(x => x.Location, 0, 0, nulls);
        }

        var queryResults = await query.ToListAsync();
        AssertSpatialNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanMixPerClauseNullsInMultiSortViaRql(Options options, bool primaryNullsFirst, bool secondaryNullsFirst, bool isAutoIndex)
    {
        using var store = await CreateMixedNullsFixture(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} where exists(id()) order by Name asc {NullFirstClause(primaryNullsFirst)}, IntValue as long asc {NullFirstClause(secondaryNullsFirst)}";

        var results = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();
        AssertMixedNullsOrdering(results, primaryNullsFirst, secondaryNullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanMixPerClauseNullsInMultiSortViaLinq(Options options, bool primaryNullsFirst, bool secondaryNullsFirst, bool isAutoIndex)
    {
        using var store = await CreateMixedNullsFixture(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);

        var ordered = baseQuery
            .OrderBy(x => x.Name, ToNullsOrdering(primaryNullsFirst), OrderingType.String)
            .ThenBy(x => x.IntValue, ToNullsOrdering(secondaryNullsFirst), OrderingType.Long);

        var results = await ordered.ToListAsync();
        AssertMixedNullsOrdering(results, primaryNullsFirst, secondaryNullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanMixPerClauseNullsInMultiSortViaDocumentQuery(Options options, bool primaryNullsFirst, bool secondaryNullsFirst, bool isAutoIndex)
    {
        using var store = await CreateMixedNullsFixture(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();
        query = query
            .WhereExists(x => x.Id)
            .OrderBy(x => x.Name, ToNullsOrdering(primaryNullsFirst), OrderingType.String)
            .OrderBy(x => x.IntValue, ToNullsOrdering(secondaryNullsFirst), OrderingType.Long);

        var results = await query.ToListAsync();
        AssertMixedNullsOrdering(results, primaryNullsFirst, secondaryNullsFirst);
    }

    private static string OrderByDirection(bool ascending) => ascending ? "asc" : "desc";
    private static string NullFirstClause(bool first) => first ? "nulls first" : "nulls last";
    private static NullsOrdering ToNullsOrdering(bool first) => first ? NullsOrdering.First : NullsOrdering.Last;

    private static void AssertStringNullsOrdering(List<Document> results, bool isAscending, bool nullsFirst)
    {
        Assert.Equal(4, results.Count);
        switch (IsAscending: isAscending, NullsFirst: nullsFirst)
        {
            case (IsAscending: true, NullsFirst: true):
                Assert.Null(results[0].Name);
                Assert.Null(results[1].Name);
                Assert.Equal("a", results[2].Name);
                Assert.Equal("b", results[3].Name);
                break;
            case (IsAscending: false, NullsFirst: true):
                Assert.Null(results[0].Name);
                Assert.Null(results[1].Name);
                Assert.Equal("b", results[2].Name);
                Assert.Equal("a", results[3].Name);
                break;
            case (IsAscending: true, NullsFirst: false):
                Assert.Equal("a", results[0].Name);
                Assert.Equal("b", results[1].Name);
                Assert.Null(results[2].Name);
                Assert.Null(results[3].Name);
                break;
            case (IsAscending: false, NullsFirst: false):
                Assert.Equal("b", results[0].Name);
                Assert.Equal("a", results[1].Name);
                Assert.Null(results[2].Name);
                Assert.Null(results[3].Name);
                break;
        }
    }

    private static void AssertIntNullsOrdering(List<Document> results, bool isAscending, bool nullsFirst)
    {
        Assert.Equal(4, results.Count);
        switch (IsAscending: isAscending, NullsFirst: nullsFirst)
        {
            case (IsAscending: true, NullsFirst: true):
                Assert.Null(results[0].IntValue);
                Assert.Null(results[1].IntValue);
                Assert.Equal(1, results[2].IntValue);
                Assert.Equal(2, results[3].IntValue);
                break;
            case (IsAscending: false, NullsFirst: true):
                Assert.Null(results[0].IntValue);
                Assert.Null(results[1].IntValue);
                Assert.Equal(2, results[2].IntValue);
                Assert.Equal(1, results[3].IntValue);
                break;
            case (IsAscending: true, NullsFirst: false):
                Assert.Equal(1, results[0].IntValue);
                Assert.Equal(2, results[1].IntValue);
                Assert.Null(results[2].IntValue);
                Assert.Null(results[3].IntValue);
                break;
            case (IsAscending: false, NullsFirst: false):
                Assert.Equal(2, results[0].IntValue);
                Assert.Equal(1, results[1].IntValue);
                Assert.Null(results[2].IntValue);
                Assert.Null(results[3].IntValue);
                break;
        }
    }

    private static void AssertDoubleNullsOrdering(List<Document> results, bool isAscending, bool nullsFirst)
    {
        Assert.Equal(4, results.Count);
        switch (IsAscending: isAscending, NullsFirst: nullsFirst)
        {
            case (IsAscending: true, NullsFirst: true):
                Assert.Null(results[0].DoubleValue);
                Assert.Null(results[1].DoubleValue);
                Assert.Equal(1, results[2].DoubleValue);
                Assert.Equal(2, results[3].DoubleValue);
                break;
            case (IsAscending: false, NullsFirst: true):
                Assert.Null(results[0].DoubleValue);
                Assert.Null(results[1].DoubleValue);
                Assert.Equal(2, results[2].DoubleValue);
                Assert.Equal(1, results[3].DoubleValue);
                break;
            case (IsAscending: true, NullsFirst: false):
                Assert.Equal(1, results[0].DoubleValue);
                Assert.Equal(2, results[1].DoubleValue);
                Assert.Null(results[2].DoubleValue);
                Assert.Null(results[3].DoubleValue);
                break;
            case (IsAscending: false, NullsFirst: false):
                Assert.Equal(2, results[0].DoubleValue);
                Assert.Equal(1, results[1].DoubleValue);
                Assert.Null(results[2].DoubleValue);
                Assert.Null(results[3].DoubleValue);
                break;
        }
    }

    private static void AssertSpatialNullsOrdering(List<Document> results, bool isAscending, bool nullsFirst)
    {
        Assert.Equal(4, results.Count);
        switch (IsAscending: isAscending, NullsFirst: nullsFirst)
        {
            case (IsAscending: true, NullsFirst: true):
                Assert.Null(results[0].Location);
                Assert.Null(results[1].Location);
                AssertGeoAt(10, 10, results[2]);
                AssertGeoAt(20, 20, results[3]);
                break;
            case (IsAscending: false, NullsFirst: true):
                Assert.Null(results[0].Location);
                Assert.Null(results[1].Location);
                AssertGeoAt(20, 20, results[2]);
                AssertGeoAt(10, 10, results[3]);
                break;
            case (IsAscending: true, NullsFirst: false):
                AssertGeoAt(10, 10, results[0]);
                AssertGeoAt(20, 20, results[1]);
                Assert.Null(results[2].Location);
                Assert.Null(results[3].Location);
                break;
            case (IsAscending: false, NullsFirst: false):
                AssertGeoAt(20, 20, results[0]);
                AssertGeoAt(10, 10, results[1]);
                Assert.Null(results[2].Location);
                Assert.Null(results[3].Location);
                break;
        }

        static void AssertGeoAt(double lat, double lon, Document d)
        {
            Assert.Equal(lat, d.Location.Latitude);
            Assert.Equal(lon, d.Location.Longitude);
        }
    }

    private static void AssertMixedNullsOrdering(List<Document> results, bool primaryNullsFirst, bool secondaryNullsFirst)
    {
        Assert.Equal(4, results.Count);
        switch (PrimaryFirst: primaryNullsFirst, SecondaryFirst: secondaryNullsFirst)
        {
            case (PrimaryFirst: true, SecondaryFirst: true):
                Assert.Null(results[0].Name);
                Assert.Null(results[0].IntValue);
                Assert.Null(results[1].Name);
                Assert.Equal(2, results[1].IntValue);
                Assert.Equal("a", results[2].Name);
                Assert.Null(results[2].IntValue);
                Assert.Equal("a", results[3].Name);
                Assert.Equal(1, results[3].IntValue);
                break;
            case (PrimaryFirst: true, SecondaryFirst: false):
                Assert.Null(results[0].Name);
                Assert.Equal(2, results[0].IntValue);
                Assert.Null(results[1].Name);
                Assert.Null(results[1].IntValue);
                Assert.Equal("a", results[2].Name);
                Assert.Equal(1, results[2].IntValue);
                Assert.Equal("a", results[3].Name);
                Assert.Null(results[3].IntValue);
                break;
            case (PrimaryFirst: false, SecondaryFirst: true):
                Assert.Equal("a", results[0].Name);
                Assert.Null(results[0].IntValue);
                Assert.Equal("a", results[1].Name);
                Assert.Equal(1, results[1].IntValue);
                Assert.Null(results[2].Name);
                Assert.Null(results[2].IntValue);
                Assert.Null(results[3].Name);
                Assert.Equal(2, results[3].IntValue);
                break;
            case (PrimaryFirst: false, SecondaryFirst: false):
                Assert.Equal("a", results[0].Name);
                Assert.Equal(1, results[0].IntValue);
                Assert.Equal("a", results[1].Name);
                Assert.Null(results[1].IntValue);
                Assert.Null(results[2].Name);
                Assert.Equal(2, results[2].IntValue);
                Assert.Null(results[3].Name);
                Assert.Null(results[3].IntValue);
                break;
        }
    }

    private async Task<DocumentStore> CreateDocumentsAndIndexes(Options options, bool autoIndex)
    {
        var store = GetDocumentStore(options);
        using var session = store.OpenAsyncSession();

        var nullDocument = new Document { Name = null, DoubleValue = null, IntValue = null, Location = null, ToIgnore = nameof(Document.ToIgnore) };
        var oneDocument  = new Document { Name = "a",  DoubleValue = 1,    IntValue = 1,    Location = new Geo { Latitude = 10, Longitude = 10 }, ToIgnore = nameof(Document.ToIgnore) };
        var twoDocument  = new Document { Name = "b",  DoubleValue = 2,    IntValue = 2,    Location = new Geo { Latitude = 20, Longitude = 20 }, ToIgnore = nameof(Document.ToIgnore) };
        var nonExisting  = new Document { Name = null, DoubleValue = null, IntValue = null, Location = null, ToIgnore = nameof(Document.ToIgnore) };
        await session.StoreAsync(nullDocument);
        await session.StoreAsync(oneDocument);
        await session.StoreAsync(twoDocument);
        await session.StoreAsync(nonExisting);
        await session.SaveChangesAsync();

        var operation = await store.Operations.SendAsync(new PatchByQueryOperation(
            $@"from Documents where id() == '{nonExisting.Id}' update {{
    delete(this['Name']);
    delete(this['DoubleValue']);
    delete(this['IntValue']);
    delete(this['Location']);
}}"));
        await operation.WaitForCompletionAsync();

        if (autoIndex == false)
        {
            var index = new DocumentIndex();
            await index.ExecuteAsync(store);
        }
        else
        {
            var _ = await session.Query<Document>()
                .Statistics(out var stats)
                .Where(x => x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null)
                .Spatial(x => x.Point(d => d.Location.Latitude, d => d.Location.Longitude), f => f.WithinRadius(1000, 0, 0))
                .ToListAsync();
        }

        await Indexes.WaitForIndexingAsync(store);
        return store;
    }

    private async Task<DocumentStore> CreateMixedNullsFixture(Options options, bool autoIndex)
    {
        var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Document { Name = "a",  IntValue = null, ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Name = null, IntValue = 2,    ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Name = "a",  IntValue = 1,    ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Name = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) });
            await session.SaveChangesAsync();
        }

        if (autoIndex == false)
        {
            var index = new DocumentIndex();
            await index.ExecuteAsync(store);
        }
        else
        {
            using var session = store.OpenAsyncSession();
            var _ = await session.Query<Document>()
                .Statistics(out var stats)
                .Where(x => x.Name == "1" || x.IntValue > 0 || x.ToIgnore == null)
                .ToListAsync();
        }

        await Indexes.WaitForIndexingAsync(store);
        return store;
    }

    private class Document
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int? IntValue { get; set; }
        public double? DoubleValue { get; set; }
        public string ToIgnore { get; set; }
        public Geo Location { get; set; }
    }

    private class Geo
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    private class DocumentIndex : AbstractIndexCreationTask<Document>
    {
        public DocumentIndex()
        {
            Map = docs => from doc in docs
                select new
                {
                    doc.Name,
                    doc.IntValue,
                    doc.DoubleValue,
                    doc.ToIgnore,
                    Location = doc.Location == null ? null : CreateSpatialField(doc.Location.Latitude, doc.Location.Longitude)
                };
        }
    }
}
