using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Corax.Utils;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

public class RavenDB_26091(ITestOutputHelper output) : RavenTestBase(output)
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
    public async Task CanChangeOrderOfTheNullsWhenSortingString(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();


        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("Name", root.Parameters["FieldName"]);
            Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
            Assert.Equal("Sequence", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("a", queryResults[2].Name);
                Assert.Equal("b", queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal("b", queryResults[0].Name);
                Assert.Equal("a", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal("a", queryResults[0].Name);
                Assert.Equal("b", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("b", queryResults[2].Name);
                Assert.Equal("a", queryResults[3].Name);
                break;
        }
        
        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.WhereExists(x => x.Id);
            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.Name)
                : query.OrderByDescending(x => x.Name);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenSortingInt(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("IntValue", root.Parameters["FieldName"]);
            Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
            Assert.Equal("Integer", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].IntValue);
                Assert.Null(queryResults[1].IntValue);
                Assert.Equal(1, queryResults[2].IntValue);
                Assert.Equal(2, queryResults[3].IntValue);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal(2, queryResults[0].IntValue);
                Assert.Equal(1, queryResults[1].IntValue);
                Assert.Null(queryResults[2].IntValue);
                Assert.Null(queryResults[3].IntValue);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal(1, queryResults[0].IntValue);
                Assert.Equal(2, queryResults[1].IntValue);
                Assert.Null(queryResults[2].IntValue);
                Assert.Null(queryResults[3].IntValue);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].IntValue);
                Assert.Null(queryResults[1].IntValue);
                Assert.Equal(2, queryResults[2].IntValue);
                Assert.Equal(1, queryResults[3].IntValue);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.WhereExists(x => x.Id);
            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.IntValue, OrderingType.Long)
                : query.OrderByDescending(x => x.IntValue, OrderingType.Long);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenSortingDouble(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool forceSortUsingIndex)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("DoubleValue", root.Parameters["FieldName"]);
            Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
            Assert.Equal("Floating", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].DoubleValue);
                Assert.Null(queryResults[1].DoubleValue);
                Assert.Equal(1, queryResults[2].DoubleValue);
                Assert.Equal(2, queryResults[3].DoubleValue);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal(2, queryResults[0].DoubleValue);
                Assert.Equal(1, queryResults[1].DoubleValue);
                Assert.Null(queryResults[2].DoubleValue);
                Assert.Null(queryResults[3].DoubleValue);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal(1, queryResults[0].DoubleValue);
                Assert.Equal(2, queryResults[1].DoubleValue);
                Assert.Null(queryResults[2].DoubleValue);
                Assert.Null(queryResults[3].DoubleValue);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].DoubleValue);
                Assert.Null(queryResults[1].DoubleValue);
                Assert.Equal(2, queryResults[2].DoubleValue);
                Assert.Equal(1, queryResults[3].DoubleValue);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.WhereExists(x => x.Id);
            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.DoubleValue, OrderingType.Double)
                : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingString(Options options, bool nullFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("a", queryResults[2].Name);
                Assert.Equal("b", queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal("b", queryResults[0].Name);
                Assert.Equal("a", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal("a", queryResults[0].Name);
                Assert.Equal("b", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("b", queryResults[2].Name);
                Assert.Equal("a", queryResults[3].Name);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.Name)
                : query.OrderByDescending(x => x.Name);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingInt(Options options, bool nullFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].IntValue);
                Assert.Null(queryResults[1].IntValue);
                Assert.Equal(1, queryResults[2].IntValue);
                Assert.Equal(2, queryResults[3].IntValue);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal(2, queryResults[0].IntValue);
                Assert.Equal(1, queryResults[1].IntValue);
                Assert.Null(queryResults[2].IntValue);
                Assert.Null(queryResults[3].IntValue);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal(1, queryResults[0].IntValue);
                Assert.Equal(2, queryResults[1].IntValue);
                Assert.Null(queryResults[2].IntValue);
                Assert.Null(queryResults[3].IntValue);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].IntValue);
                Assert.Null(queryResults[1].IntValue);
                Assert.Equal(2, queryResults[2].IntValue);
                Assert.Equal(1, queryResults[3].IntValue);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.IntValue, OrderingType.Long)
                : query.OrderByDescending(x => x.IntValue, OrderingType.Long);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingDouble(Options options, bool nullFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].DoubleValue);
                Assert.Null(queryResults[1].DoubleValue);
                Assert.Equal(1, queryResults[2].DoubleValue);
                Assert.Equal(2, queryResults[3].DoubleValue);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal(2, queryResults[0].DoubleValue);
                Assert.Equal(1, queryResults[1].DoubleValue);
                Assert.Null(queryResults[2].DoubleValue);
                Assert.Null(queryResults[3].DoubleValue);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal(1, queryResults[0].DoubleValue);
                Assert.Equal(2, queryResults[1].DoubleValue);
                Assert.Null(queryResults[2].DoubleValue);
                Assert.Null(queryResults[3].DoubleValue);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].DoubleValue);
                Assert.Null(queryResults[1].DoubleValue);
                Assert.Equal(2, queryResults[2].DoubleValue);
                Assert.Equal(1, queryResults[3].DoubleValue);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.DoubleValue, OrderingType.Double)
                : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenSortingAlphaNumeric(Options options, bool nullFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();
        var queryResults = await CreateQuery(out var timings)
            .ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("Name", root.Parameters["FieldName"]);
            Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
            Assert.Equal("Alphanumeric", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("a", queryResults[2].Name);
                Assert.Equal("b", queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: true):
                Assert.Equal("b", queryResults[0].Name);
                Assert.Equal("a", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: true, NullFirst: false):
                Assert.Equal("a", queryResults[0].Name);
                Assert.Equal("b", queryResults[1].Name);
                Assert.Null(queryResults[2].Name);
                Assert.Null(queryResults[3].Name);
                break;
            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].Name);
                Assert.Null(queryResults[1].Name);
                Assert.Equal("b", queryResults[2].Name);
                Assert.Equal("a", queryResults[3].Name);
                break;
        }

        IAsyncDocumentQuery<Document> CreateQuery(out QueryTimings timings)
        {
            IAsyncDocumentQuery<Document> query = isAutoIndex
                ? session.Advanced.AsyncDocumentQuery<Document>()
                : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

            query = query.WhereExists(x => x.Id);
            query = query.Timings(out timings);
            query = isAscending
                ? query.OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                : query.OrderByDescending(x => x.Name, OrderingType.AlphaNumeric);
            return query;
        }
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
    public async Task CanChangeOrderOfTheNullsWhenSortingSpatial(Options options, bool nullFirst, bool isAutoIndex, bool isAscending)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, true, forceSortUsingIndex: false);
        using var session = store.OpenAsyncSession();
        WaitForUserToContinueTheTest(store);
        var orderClause = isAscending ? "" : " desc";
        var rql = isAutoIndex
            ? $"from Documents where exists(ToIgnore) order by spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0)) {orderClause}"
            : $"from index '{new DocumentIndex().IndexName}' where exists(ToIgnore) order by spatial.distance(Location, spatial.point(0,0)){orderClause}";

        rql += " include timings()";
        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).Timings(out var timings).ToListAsync();

        if (options.DatabaseMode != RavenDatabaseMode.Sharded)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal(isAutoIndex ? "spatial.point(Location.Latitude, Location.Longitude)" : "Location", root.Parameters["FieldName"]);
            Assert.Equal(isAscending.ToString(), root.Parameters["Ascending"]);
            Assert.Equal("Spatial", root.Parameters["FieldType"]);
            Assert.Equal("Pt(x=0.0,y=0.0)", root.Parameters["Point"]);
            Assert.Equal("Kilometers", root.Parameters["Units"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        }

        Assert.Equal(4, queryResults.Count);

        switch (IsAscending: isAscending, NullFirst: nullFirst)
        {
            case (IsAscending: true, NullFirst: true):
                Assert.Null(queryResults[0].Location);
                Assert.Null(queryResults[1].Location);
                Assert.Equal(expected: 10, actual: queryResults[2].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[2].Location.Longitude);
                Assert.Equal(expected: 20, actual: queryResults[3].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[3].Location.Longitude);
                break;

            case (IsAscending: true, NullFirst: false):
                Assert.Equal(expected: 10, actual: queryResults[0].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[0].Location.Longitude);
                Assert.Equal(expected: 20, actual: queryResults[1].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[1].Location.Longitude);
                Assert.Null(queryResults[2].Location);
                Assert.Null(queryResults[3].Location);
                break;

            case (IsAscending: false, NullFirst: true):
                Assert.Equal(expected: 20, actual: queryResults[0].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[0].Location.Longitude);
                Assert.Equal(expected: 10, actual: queryResults[1].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[1].Location.Longitude);
                Assert.Null(queryResults[2].Location);
                Assert.Null(queryResults[3].Location);
                break;

            case (IsAscending: false, NullFirst: false):
                Assert.Null(queryResults[0].Location);
                Assert.Null(queryResults[1].Location);
                Assert.Equal(expected: 20, actual: queryResults[2].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[2].Location.Longitude);
                Assert.Equal(expected: 10, actual: queryResults[3].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[3].Location.Longitude);
                break;
        }
    }


    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false])]
    public async Task AlphanumericalNullPagingTest(Options options, bool nullFirst)
    {
        options.ModifyDatabaseRecord += record =>
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullsSortMode)] = nullFirst
                ? Raven.Client.Documents.Indexes.NullsSortMode.NullsSmallest.ToString()
                : Raven.Client.Documents.Indexes.NullsSortMode.NullsLargest.ToString();

        using var store = GetDocumentStore(options);
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new Document { Name = null, ToIgnore = nameof(Document.ToIgnore) });
        await session.StoreAsync(new Document { Name = null, ToIgnore = nameof(Document.ToIgnore) });
        await session.StoreAsync(new Document { Name = "aaa", ToIgnore = nameof(Document.ToIgnore) });
        await session.StoreAsync(new Document { Name = "bbb", ToIgnore = nameof(Document.ToIgnore) });
        await session.StoreAsync(new Document { Name = "ccc", ToIgnore = nameof(Document.ToIgnore) });
        await session.SaveChangesAsync();

        var result = await session.Query<Document>().Customize(x => x.WaitForNonStaleResults())
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .ToListAsync();

        Assert.Equal(5, result.Count);
        if (nullFirst)
        {
            Assert.Null(result[0].Name);
            Assert.Null(result[1].Name);
            Assert.Equal("aaa", result[2].Name);
            Assert.Equal("bbb", result[3].Name);
            Assert.Equal("ccc", result[4].Name);
        }
        else
        {
            Assert.Equal("aaa", result[0].Name);
            Assert.Equal("bbb", result[1].Name);
            Assert.Equal("ccc", result[2].Name);
            Assert.Null(result[3].Name);
            Assert.Null(result[4].Name);
        }

        result = await session.Query<Document>()
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .Take(1)
            .ToListAsync();
        Assert.Equal(1, result.Count);
        if (nullFirst)
            Assert.Null(result[0].Name);
        else
            Assert.Equal("aaa", result[0].Name);

        result = await session.Query<Document>()
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .Skip(3)
            .Take(1)
            .ToListAsync();
        Assert.Equal(1, result.Count);
        if (nullFirst)
            Assert.Equal("bbb", result[0].Name);
        else
            Assert.Null(result[0].Name);

        result = await session.Query<Document>()
            .OrderByDescending(x => x.Name, OrderingType.AlphaNumeric)
            .Skip(3)
            .Take(10)
            .ToListAsync();
        Assert.Equal(2, result.Count);
        if (nullFirst)
        {
            Assert.Null(result[0].Name);
            Assert.Null(result[1].Name);
        }
        else
        {
            Assert.Equal("bbb", result[0].Name);
            Assert.Equal("aaa", result[1].Name);
        }
        
        result = await session.Query<Document>()
            .OrderByDescending(x => x.Name, OrderingType.AlphaNumeric)
            .ToListAsync();

        Assert.Equal(5, result.Count);
        if (nullFirst)
        {
            Assert.Equal("ccc", result[0].Name);
            Assert.Equal("bbb", result[1].Name);
            Assert.Equal("aaa", result[2].Name);
            Assert.Null(result[3].Name);
            Assert.Null(result[4].Name);
        }
        else
        {
            Assert.Null(result[0].Name);
            Assert.Null(result[1].Name);
            Assert.Equal("ccc", result[2].Name);
            Assert.Equal("bbb", result[3].Name);
            Assert.Equal("aaa", result[4].Name);
        }

        result = await session.Query<Document>()
            .OrderByDescending(x => x.Name, OrderingType.AlphaNumeric)
            .Take(1)
            .ToListAsync();
        Assert.Equal(1, result.Count);
        if (nullFirst)
            Assert.Equal("ccc", result[0].Name);
        else
            Assert.Null(result[0].Name);

        result = await session.Query<Document>()
            .OrderByDescending(x => x.Name, OrderingType.AlphaNumeric)
            .Skip(3)
            .Take(1)
            .ToListAsync();
        Assert.Equal(1, result.Count);
        if (nullFirst)
            Assert.Null(result[0].Name);
        else
            Assert.Equal("bbb", result[0].Name);
    }
    

    private async Task<DocumentStore> CreateDocumentsAndIndexes(Options options, bool nullFirst, bool autoIndexes, bool testNonExisting, bool forceSortUsingIndex)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullsSortMode)] = nullFirst
                ? Raven.Client.Documents.Indexes.NullsSortMode.NullsSmallest.ToString()
                : Raven.Client.Documents.Indexes.NullsSortMode.NullsLargest.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };

        var store = GetDocumentStore(options);


        using var session = store.OpenAsyncSession();


        var nullDocument = new Document { Name = null, DoubleValue = null, IntValue = null, Location = null, ToIgnore = nameof(Document.ToIgnore) };
        var oneDocument = new Document { Name = "a", DoubleValue = 1, IntValue = 1, Location = new() { Latitude = 10, Longitude = 10 }, ToIgnore = nameof(Document.ToIgnore) };
        var twoDocument = new Document { Name = "b", DoubleValue = 2, IntValue = 2, Location = new() { Latitude = 20, Longitude = 20 }, ToIgnore = nameof(Document.ToIgnore) };
        var nonExistingFields = new Document() { Name = null, DoubleValue = null, IntValue = null, Location = null, ToIgnore = nameof(Document.ToIgnore) };
        await session.StoreAsync(nullDocument);
        await session.StoreAsync(oneDocument);
        await session.StoreAsync(twoDocument);
        await session.StoreAsync(nonExistingFields);
        await session.SaveChangesAsync();

        if (testNonExisting)
        {
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(
                $@"from Documents where 
id() == '{nonExistingFields.Id}' 
update {{
    delete(this['Name']);
    delete(this['DoubleValue']);
    delete(this['IntValue']);
    delete(this['Location']);
}}"));
            await operation.WaitForCompletionAsync();
        }

        string indexName;
        if (autoIndexes == false)
        {
            var index = new DocumentIndex();
            await index.ExecuteAsync(store);
            indexName = index.IndexName;
        }
        else
        {
            // ignore value, create an autoindex
            var _ = await session.Query<Document>()
                .Statistics(out var stats)
                .Where(x => x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null)
                .Spatial(x => x.Point(doc => doc.Location.Latitude, doc => doc.Location.Longitude), f => f.WithinRadius(1000, 0, 0))
                .ToListAsync();
            indexName = stats.IndexName;
        }

        await Indexes.WaitForIndexingAsync(store);

        if (forceSortUsingIndex)
        {
            Assert.NotEqual(RavenDatabaseMode.Sharded, options.DatabaseMode);
            var db = await GetDatabase(store.Database);
            var indexInstance = db.IndexStore.GetIndex(indexName);
            indexInstance.ForTestingPurposesOnly().CoraxConfiguration = new CoraxTestingConfiguration() { ForceSortingUsingIndex = true };
        }

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
