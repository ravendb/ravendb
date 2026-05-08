using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Corax.Utils;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

public class RavenDB_26236(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenSortingStringViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} where exists(id()) order by Name {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)} include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        AssertSortingMatchPlan(options, timings, "Name", "Sequence", isAscending);
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenSortingStringViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);
        var ordered = isAscending
            ? baseQuery.OrderBy(x => x.Name, OrderingType.String, nulls)
            : baseQuery.OrderByDescending(x => x.Name, OrderingType.String, nulls);

        var queryResults = await ordered.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenSortingStringViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .WhereExists(x => x.Id);
        query = isAscending
            ? query.OrderBy(x => x.Name, OrderingType.String, nulls)
            : query.OrderByDescending(x => x.Name, OrderingType.String, nulls);

        var queryResults = await query.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingIntViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} where exists(id()) order by IntValue as long {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)} include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        AssertSortingMatchPlan(options, timings, "IntValue", "Integer", isAscending);
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingIntViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);
        var ordered = isAscending
            ? baseQuery.OrderBy(x => x.IntValue, OrderingType.Long, nulls)
            : baseQuery.OrderByDescending(x => x.IntValue, OrderingType.Long, nulls);

        var queryResults = await ordered.ToListAsync();
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingIntViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .WhereExists(x => x.Id);
        query = isAscending
            ? query.OrderBy(x => x.IntValue, OrderingType.Long, nulls)
            : query.OrderByDescending(x => x.IntValue, OrderingType.Long, nulls);

        var queryResults = await query.ToListAsync();
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingDoubleViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} where exists(id()) order by DoubleValue as double {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)} include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        AssertSortingMatchPlan(options, timings, "DoubleValue", "Floating", isAscending);
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingDoubleViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);
        var ordered = isAscending
            ? baseQuery.OrderBy(x => x.DoubleValue, OrderingType.Double, nulls)
            : baseQuery.OrderByDescending(x => x.DoubleValue, OrderingType.Double, nulls);

        var queryResults = await ordered.ToListAsync();
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingDoubleViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .WhereExists(x => x.Id);
        query = isAscending
            ? query.OrderBy(x => x.DoubleValue, OrderingType.Double, nulls)
            : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double, nulls);

        var queryResults = await query.ToListAsync();
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingAlphaNumericViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} where exists(id()) order by Name as alphanumeric {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)} include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        AssertSortingMatchPlan(options, timings, "Name", "Alphanumeric", isAscending);
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingAlphaNumericViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        baseQuery = baseQuery.Where(x => x.Id != null);
        var ordered = isAscending
            ? baseQuery.OrderBy(x => x.Name, OrderingType.AlphaNumeric, nulls)
            : baseQuery.OrderByDescending(x => x.Name, OrderingType.AlphaNumeric, nulls);

        var queryResults = await ordered.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingAlphaNumericViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .WhereExists(x => x.Id);
        query = isAscending
            ? query.OrderBy(x => x.Name, OrderingType.AlphaNumeric, nulls)
            : query.OrderByDescending(x => x.Name, OrderingType.AlphaNumeric, nulls);

        var queryResults = await query.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
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
    public async Task CanUsePerQueryNullsClauseWhenSortingSpatialViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var distance = isAutoIndex
            ? "spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0))"
            : "spatial.distance(Location, spatial.point(0,0))";
        var rql = $"{fromClause} where exists(id()) order by {distance} {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)}";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();

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
    public async Task CanUsePerQueryNullsClauseWhenSortingSpatialViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .WhereExists(x => x.Id);

        if (isAutoIndex)
        {
            query = isAscending
                ? query.OrderByDistance(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls)
                : query.OrderByDistanceDescending(f => f.Point(d => d.Location.Latitude, d => d.Location.Longitude), 0, 0, nulls);
        }
        else
        {
            query = isAscending
                ? query.OrderByDistance(x => x.Location, 0, 0, nulls)
                : query.OrderByDistanceDescending(x => x.Location, 0, 0, nulls);
        }

        var queryResults = await query.ToListAsync();
        AssertSpatialNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenStreamingSortingStringViaRql(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var fromClause = isAutoIndex ? "from Documents" : $"from index '{new DocumentIndex().IndexName}'";
        var rql = $"{fromClause} order by Name {OrderByDirection(isAscending)} {NullFirstClause(nullsFirst)} include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        AssertStreamingPlan(options, timings);
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenStreamingSortingStringViaLinq(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        IRavenQueryable<Document> baseQuery = isAutoIndex
            ? session.Query<Document>()
            : session.Query<Document, DocumentIndex>();
        var ordered = isAscending
            ? baseQuery.OrderBy(x => x.Name, OrderingType.String, nulls)
            : baseQuery.OrderByDescending(x => x.Name, OrderingType.String, nulls);

        var queryResults = await ordered.ToListAsync();
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenStreamingSortingStringViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .Timings(out var timings);
        query = isAscending
            ? query.OrderBy(x => x.Name, OrderingType.String, nulls)
            : query.OrderByDescending(x => x.Name, OrderingType.String, nulls);

        var queryResults = await query.ToListAsync();
        AssertStreamingPlan(options, timings);
        AssertStringNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenStreamingSortingIntViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .Timings(out var timings);
        query = isAscending
            ? query.OrderBy(x => x.IntValue, OrderingType.Long, nulls)
            : query.OrderByDescending(x => x.IntValue, OrderingType.Long, nulls);

        var queryResults = await query.ToListAsync();
        AssertStreamingPlan(options, timings);
        AssertIntNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false, false])]
    public async Task CanUsePerQueryNullsClauseWhenStreamingSortingDoubleViaDocumentQuery(Options options, bool nullsFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, isAutoIndex, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();

        var nulls = ToNullsOrdering(nullsFirst);
        var query = (isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>())
            .Timings(out var timings);
        query = isAscending
            ? query.OrderBy(x => x.DoubleValue, OrderingType.Double, nulls)
            : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double, nulls);

        var queryResults = await query.ToListAsync();
        AssertStreamingPlan(options, timings);
        AssertDoubleNullsOrdering(queryResults, isAscending, nullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false, false])]
    public async Task MultiClauseOrderByHonorsPerClauseNullsFirstLast(Options options, bool primaryNullsFirst, bool secondaryNullsFirst)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Document { Id = "docs/1", Name = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Id = "docs/2", Name = null, IntValue = 10,   ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Id = "docs/3", Name = "x",  IntValue = null, ToIgnore = nameof(Document.ToIgnore) });
            await session.StoreAsync(new Document { Id = "docs/4", Name = "x",  IntValue = 20,   ToIgnore = nameof(Document.ToIgnore) });
            await session.SaveChangesAsync();
        }
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            var rql = $"from Documents order by Name desc {NullFirstClause(primaryNullsFirst)}, IntValue as long desc {NullFirstClause(secondaryNullsFirst)}";
            var results = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();

            Assert.Equal(4, results.Count);

            var nullGroup = secondaryNullsFirst ? new[] { "docs/1", "docs/2" } : new[] { "docs/2", "docs/1" };
            var xGroup    = secondaryNullsFirst ? new[] { "docs/3", "docs/4" } : new[] { "docs/4", "docs/3" };
            var expected = primaryNullsFirst
                ? nullGroup.Concat(xGroup).ToArray()
                : xGroup.Concat(nullGroup).ToArray();

            Assert.Equal(expected, results.Select(d => d.Id).ToArray());
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false])]
    public async Task QueryClauseOverridesIndexConfig(Options options, bool configNullIsSmallest)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullsSortMode)] = configNullIsSmallest
                ? Raven.Client.Documents.Indexes.NullsSortMode.NullsSmallest.ToString()
                : Raven.Client.Documents.Indexes.NullsSortMode.NullsLargest.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };
        using var store = GetDocumentStore(options);
        await StoreFixtureDocuments(store);
        var index = new DocumentIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using var session = store.OpenAsyncSession();

        var queryNullsFirst = !configNullIsSmallest;
        var rql = $"from index '{index.IndexName}' where exists(id()) order by Name asc {NullFirstClause(queryNullsFirst)}";
        var results = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();

        AssertStringNullsOrdering(results, isAscending: true, nullsFirst: queryNullsFirst);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Configuration)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false])]
    public async Task LegacyNullFirstConfigKeyStillBindsToNewEnum(Options options, bool legacyNullFirst)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullFirstLegacy)] = legacyNullFirst.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };
        using var store = GetDocumentStore(options);
        var index = new DocumentIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var indexInstance = database.IndexStore.GetIndex(index.IndexName);

        var expected = legacyNullFirst
            ? Raven.Client.Documents.Indexes.NullsSortMode.NullsSmallest
            : Raven.Client.Documents.Indexes.NullsSortMode.NullsLargest;
        Assert.Equal(expected, indexInstance.Configuration.NullsSortMode);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = ["nulls first"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = ["nulls last"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = ["asc nulls first"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = ["desc nulls last"])]
    public async Task OrderByNullsClauseOnLuceneIndexThrows(Options options, string nullsClause)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Document { Name = "a", ToIgnore = nameof(Document.ToIgnore) });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var rql = $"from Documents order by Name {nullsClause}";
            var ex = await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync());

            Assert.Contains("NULLS FIRST", ex.Message);
            Assert.Contains("Lucene", ex.Message);
            Assert.Contains("Corax", ex.Message);
        }
    }

    private static string OrderByDirection(bool ascending) => ascending ? "asc" : "desc";
    private static string NullFirstClause(bool first) => first ? "nulls first" : "nulls last";
    private static NullsOrdering ToNullsOrdering(bool first) => first ? NullsOrdering.First : NullsOrdering.Last;

    private static void AssertSortingMatchPlan(Options options, QueryTimings timings, string fieldName, string fieldType, bool isAscending)
    {
        if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            return;
        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.Equal("SortingMatch", root.Operation);
        Assert.Equal(fieldName, root.Parameters["FieldName"]);
        Assert.Equal(fieldType, root.Parameters["FieldType"]);
        Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
    }

    private static void AssertStreamingPlan(Options options, QueryTimings timings)
    {
        if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            return;
        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);
    }

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

    private async Task<DocumentStore> CreateDocumentsAndIndexes(Options options, bool autoIndexes, bool forceSortUsingIndex)
    {
        var store = GetDocumentStore(options);
        await StoreFixtureDocuments(store);

        string indexName;
        if (autoIndexes == false)
        {
            var index = new DocumentIndex();
            await index.ExecuteAsync(store);
            indexName = index.IndexName;
        }
        else
        {
            using var session = store.OpenAsyncSession();
            var _ = await session.Query<Document>()
                .Statistics(out var stats)
                .Where(x => x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null)
                .Spatial(x => x.Point(d => d.Location.Latitude, d => d.Location.Longitude), f => f.WithinRadius(1000, 0, 0))
                .ToListAsync();
            indexName = stats.IndexName;
        }

        await Indexes.WaitForIndexingAsync(store);

        if (forceSortUsingIndex)
        {
            Assert.NotEqual(RavenDatabaseMode.Sharded, options.DatabaseMode);
            var db = await GetDatabase(store.Database);
            var indexInstance = db.IndexStore.GetIndex(indexName);
            indexInstance.ForTestingPurposesOnly().CoraxConfiguration = new CoraxTestingConfiguration { ForceSortingUsingIndex = true };
        }

        return store;
    }

    private static async Task StoreFixtureDocuments(DocumentStore store)
    {
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
