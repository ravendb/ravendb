using System;
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

namespace SlowTests.Corax;

public class RavenDB_26091(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingStringAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingStringBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex: true, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingStringStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingStringBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingStringBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.Name)
            .ToListAsync();


      
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("Name", root.Parameters["FieldName"]);
            Assert.Equal("True", root.Parameters["Ascending"]);
            Assert.Equal("Sequence", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("a", queryResults[2].Name);
            Assert.Equal("b", queryResults[3].Name);
        }
        else
        {
            Assert.Equal("a", queryResults[0].Name);
            Assert.Equal("b", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingIntAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingIntBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingIntStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingIntBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingIntBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.IntValue, OrderingType.Long)
            .ToListAsync();


            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("IntValue", root.Parameters["FieldName"]);
            Assert.Equal("True", root.Parameters["Ascending"]);
            Assert.Equal("Integer", root.Parameters["FieldType"]);

            Assert.Equal(1, root.Children.Count);

            var secondLevel = root.Children[0];
            Assert.Equal("MultiTermMatch", secondLevel.Operation);
            Assert.Equal(1, secondLevel.Children.Count);

            var thirdLevel = secondLevel.Children[0];
            Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
            Assert.Empty(thirdLevel.Children);
        

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(1, queryResults[2].IntValue);
            Assert.Equal(2, queryResults[3].IntValue);
        }
        else
        {
            Assert.Equal(1, queryResults[0].IntValue);
            Assert.Equal(2, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingDoubleAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingDoubleBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingDoubleStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingDoubleBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingDoubleBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.DoubleValue, OrderingType.Double)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.Equal("SortingMatch", root.Operation);
        Assert.Contains("FieldName", root.Parameters);
        Assert.Equal("DoubleValue", root.Parameters["FieldName"]);
        Assert.Equal("True", root.Parameters["Ascending"]);
        Assert.Equal("Floating", root.Parameters["FieldType"]);

        Assert.Equal(1, root.Children.Count);

        var secondLevel = root.Children[0];
        Assert.Equal("MultiTermMatch", secondLevel.Operation);
        Assert.Equal(1, secondLevel.Children.Count);

        var thirdLevel = secondLevel.Children[0];
        Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
        Assert.Empty(thirdLevel.Children);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(1, queryResults[2].DoubleValue);
            Assert.Equal(2, queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Equal(1, queryResults[0].DoubleValue);
            Assert.Equal(2, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.Name)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("a", queryResults[2].Name);
            Assert.Equal("b", queryResults[3].Name);
        }
        else
        {
            Assert.Equal("a", queryResults[0].Name);
            Assert.Equal("b", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.IntValue, OrderingType.Long)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(1, queryResults[2].IntValue);
            Assert.Equal(2, queryResults[3].IntValue);
        }
        else
        {
            Assert.Equal(1, queryResults[0].IntValue);
            Assert.Equal(2, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.DoubleValue, OrderingType.Double)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(1, queryResults[2].DoubleValue);
            Assert.Equal(2, queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Equal(1, queryResults[0].DoubleValue);
            Assert.Equal(2, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingStringDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingStringDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.Name)
            .ToListAsync();


        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.Equal("SortingMatch", root.Operation);
        Assert.Contains("FieldName", root.Parameters);
        Assert.Equal("Name", root.Parameters["FieldName"]);
        Assert.Equal("False", root.Parameters["Ascending"]);
        Assert.Equal("Sequence", root.Parameters["FieldType"]);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal("b", queryResults[0].Name);
            Assert.Equal("a", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
        else
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("b", queryResults[2].Name);
            Assert.Equal("a", queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingIntDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingIntDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.IntValue, OrderingType.Long)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.Equal("SortingMatch", root.Operation);
        Assert.Contains("FieldName", root.Parameters);
        Assert.Equal("IntValue", root.Parameters["FieldName"]);
        Assert.Equal("False", root.Parameters["Ascending"]);
        Assert.Equal("Integer", root.Parameters["FieldType"]);

        Assert.Equal(1, root.Children.Count);

        var secondLevel = root.Children[0];
        Assert.Equal("MultiTermMatch", secondLevel.Operation);
        Assert.Equal(1, secondLevel.Children.Count);

        var thirdLevel = secondLevel.Children[0];
        Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
        Assert.Empty(thirdLevel.Children);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal(2, queryResults[0].IntValue);
            Assert.Equal(1, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
        else
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(2, queryResults[2].IntValue);
            Assert.Equal(1, queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingDoubleDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenSortingDoubleDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.Equal("SortingMatch", root.Operation);
        Assert.Contains("FieldName", root.Parameters);
        Assert.Equal("DoubleValue", root.Parameters["FieldName"]);
        Assert.Equal("False", root.Parameters["Ascending"]);
        Assert.Equal("Floating", root.Parameters["FieldType"]);

        Assert.Equal(1, root.Children.Count);

        var secondLevel = root.Children[0];
        Assert.Equal("MultiTermMatch", secondLevel.Operation);
        Assert.Equal(1, secondLevel.Children.Count);

        var thirdLevel = secondLevel.Children[0];
        Assert.Equal("ExistsTermProvider", thirdLevel.Operation);
        Assert.Empty(thirdLevel.Children);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal(2, queryResults[0].DoubleValue);
            Assert.Equal(1, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(2, queryResults[2].DoubleValue);
            Assert.Equal(1, queryResults[3].DoubleValue);
        }
    }


    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.Name)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal("b", queryResults[0].Name);
            Assert.Equal("a", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
        else
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("b", queryResults[2].Name);
            Assert.Equal("a", queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.IntValue, OrderingType.Long)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal(2, queryResults[0].IntValue);
            Assert.Equal(1, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
        else
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(2, queryResults[2].IntValue);
            Assert.Equal(1, queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingAutoIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(options, nullFirst, testNonExisting: true, true, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    public async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingStaticIndex(Options options, bool nullFirst, bool forceSortUsingIndex) => await
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(options, nullFirst, testNonExisting: true, false, forceSortUsingIndex, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, bool forceSortUsingIndex, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting, forceSortUsingIndex);
        using var session = store.OpenAsyncSession();

        var queryResults = await queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
            .ToListAsync();

        var root = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotEqual("SortingMatch", root.Operation);

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Equal(2, queryResults[0].DoubleValue);
            Assert.Equal(1, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(2, queryResults[2].DoubleValue);
            Assert.Equal(1, queryResults[3].DoubleValue);
        }
    }

    private async Task<DocumentStore> CreateDocumentsAndIndexes(Options options, bool nullFirst, bool autoIndexes, bool testNonExisting, bool forceSortUsingIndex)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullFirst)] = nullFirst.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };

        var store = GetDocumentStore(options);


        using var session = store.OpenAsyncSession();


        var nullDocument = new Document { Name = null, DoubleValue = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) };
        var oneDocument = new Document { Name = "a", DoubleValue = 1, IntValue = 1, ToIgnore = nameof(Document.ToIgnore) };
        var twoDocument = new Document { Name = "b", DoubleValue = 2, IntValue = 2, ToIgnore = nameof(Document.ToIgnore) };
        var nonExistingFields = new Document() { Name = null, DoubleValue = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) };
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
            var _ = await session.Query<Document>().Statistics(out var stats)
                .CountAsync(x => (x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null));
            indexName = stats.IndexName;
        }

        await Indexes.WaitForIndexingAsync(store);

        if (forceSortUsingIndex)
        {
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
    }

    private class DocumentIndex : AbstractIndexCreationTask<Document>
    {
        public DocumentIndex()
        {
            Map = docs => from doc in docs select new { doc.Name, doc.IntValue, doc.DoubleValue, doc.ToIgnore };
        }
    }
}
