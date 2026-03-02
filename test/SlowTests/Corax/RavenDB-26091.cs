using System;
using System.Linq;
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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingStringAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingStringBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingStringStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingStringBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingStringBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.Name)
            .ToList();


        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
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
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingIntAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingIntBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingIntStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingIntBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingIntBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.IntValue, OrderingType.Long)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
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
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingDoubleAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingDoubleBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingDoubleStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingDoubleBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingDoubleBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderBy(x => x.DoubleValue, OrderingType.Double)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
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
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingStringAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingStringStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingStringBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        // StartsWith query can use streaming optimization - no runtime sorting needed
        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.Name)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            // Streaming optimization should be applied - root should NOT be SortingMatch
            Assert.NotEqual("SortingMatch", root.Operation);
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingIntAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingIntStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingIntBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        // Range query can use streaming optimization - no runtime sorting needed
        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.IntValue, OrderingType.Long)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            // Streaming optimization should be applied - root should NOT be SortingMatch
            Assert.NotEqual("SortingMatch", root.Operation);
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        // Range query can use streaming optimization - no runtime sorting needed
        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderBy(x => x.DoubleValue, OrderingType.Double)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            // Streaming optimization should be applied - root should NOT be SortingMatch
            Assert.NotEqual("SortingMatch", root.Operation);
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingStringDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingStringDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingStringDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.Name)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.Equal("SortingMatch", root.Operation);
            Assert.Contains("FieldName", root.Parameters);
            Assert.Equal("Name", root.Parameters["FieldName"]);
            Assert.Equal("False", root.Parameters["Ascending"]);
            Assert.Equal("Sequence", root.Parameters["FieldType"]);
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("b", queryResults[2].Name);
            Assert.Equal("a", queryResults[3].Name);
        }
        else
        {
            Assert.Equal("b", queryResults[0].Name);
            Assert.Equal("a", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingIntDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingIntDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingIntDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.IntValue, OrderingType.Long)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
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
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(2, queryResults[2].IntValue);
            Assert.Equal(1, queryResults[3].IntValue);
        }
        else
        {
            Assert.Equal(2, queryResults[0].IntValue);
            Assert.Equal(1, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingDoubleDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenSortingDoubleDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenSortingDoubleDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .WhereExists(x => x.Id)
            .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
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
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(2, queryResults[2].DoubleValue);
            Assert.Equal(1, queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Equal(2, queryResults[0].DoubleValue);
            Assert.Equal(1, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
    }

    // Streaming Descending tests - data is fetched directly from BTree without runtime sorting

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingStringDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.Name)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].Name);
            Assert.Null(queryResults[1].Name);
            Assert.Equal("b", queryResults[2].Name);
            Assert.Equal("a", queryResults[3].Name);
        }
        else
        {
            Assert.Equal("b", queryResults[0].Name);
            Assert.Equal("a", queryResults[1].Name);
            Assert.Null(queryResults[2].Name);
            Assert.Null(queryResults[3].Name);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingIntDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.IntValue, OrderingType.Long)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].IntValue);
            Assert.Null(queryResults[1].IntValue);
            Assert.Equal(2, queryResults[2].IntValue);
            Assert.Equal(1, queryResults[3].IntValue);
        }
        else
        {
            Assert.Equal(2, queryResults[0].IntValue);
            Assert.Equal(1, queryResults[1].IntValue);
            Assert.Null(queryResults[2].IntValue);
            Assert.Null(queryResults[3].IntValue);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingAutoIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(options, nullFirst, testNonExisting, true, session => session.Advanced.DocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded, Data = [true, false])]
    public void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingStaticIndex(Options options, bool nullFirst, bool testNonExisting) =>
        CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(options, nullFirst, testNonExisting, false, session => session.Advanced.DocumentQuery<Document, DocumentIndex>());

    private void CanChangeOrderOfTheNullsWhenStreamingSortingDoubleDescendingBase(Options options, bool nullFirst, bool testNonExisting, bool isAutoIndex, Func<IDocumentSession, IDocumentQuery<Document>> queryCreator)
    {
        using var store = CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex, testNonExisting);
        using var session = store.OpenSession();

        var queryResults = queryCreator(session)
            .Timings(out var timings)
            .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
            .ToList();

        if (options.DatabaseMode is RavenDatabaseMode.Single)
        {
            var root = (QueryInspectionNode)timings.QueryPlan;
            Assert.NotEqual("SortingMatch", root.Operation);
        }

        Assert.Equal(4, queryResults.Count);

        if (nullFirst)
        {
            Assert.Null(queryResults[0].DoubleValue);
            Assert.Null(queryResults[1].DoubleValue);
            Assert.Equal(2, queryResults[2].DoubleValue);
            Assert.Equal(1, queryResults[3].DoubleValue);
        }
        else
        {
            Assert.Equal(2, queryResults[0].DoubleValue);
            Assert.Equal(1, queryResults[1].DoubleValue);
            Assert.Null(queryResults[2].DoubleValue);
            Assert.Null(queryResults[3].DoubleValue);
        }
    }

    private DocumentStore CreateDocumentsAndIndexes(Options options, bool nullFirst, bool autoIndexes, bool testNonExistsing)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullFirst)] = nullFirst.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        };

        var store = GetDocumentStore(options);


        using var session = store.OpenSession();


        var nullDocument = new Document { Name = null, DoubleValue = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) };
        var oneDocument = new Document { Name = "a", DoubleValue = 1, IntValue = 1, ToIgnore = nameof(Document.ToIgnore) };
        var twoDocument = new Document { Name = "b", DoubleValue = 2, IntValue = 2, ToIgnore = nameof(Document.ToIgnore) };
        var nonExistingFields = new Document() { Name = null, DoubleValue = null, IntValue = null, ToIgnore = nameof(Document.ToIgnore) };
        session.Store(nullDocument);
        session.Store(oneDocument);
        session.Store(twoDocument);
        session.Store(nonExistingFields);
        session.SaveChanges();

        if (testNonExistsing)
        {
            var operation = store.Operations.Send(new PatchByQueryOperation(
                $@"from Documents where 
id() == '{nonExistingFields.Id}' 
update {{
    delete(this['Name']);
    delete(this['DoubleValue']);
    delete(this['IntValue']);
}}"));
            operation.WaitForCompletion();
        }

        if (autoIndexes == false)
        {
            new DocumentIndex().Execute(store);
        }
        else
        {
            // ignore value, create an autoindex
            var _ = session.Query<Document>()
                .Count(x => (x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null));
        }

        Indexes.WaitForIndexing(store);

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
