using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

public class RavenDB_26091_MultiSorting(ITestOutputHelper output) : RavenTestBase(output)
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
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingString(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

        query = query.WhereExists(x => x.Id);
        if (fieldFirst)
        {
            query = isAscending ? query.OrderBy(x => x.Name) : query.OrderByDescending(x => x.Name);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending ? query.OrderBy(x => x.Name) : query.OrderByDescending(x => x.Name);
        }
        var queryResults = await query.ToListAsync();

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
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingInt(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

        query = query.WhereExists(x => x.Id);
        if (fieldFirst)
        {
            query = isAscending 
                ? query.OrderBy(x => x.IntValue, OrderingType.Long) 
                : query.OrderByDescending(x => x.IntValue, OrderingType.Long);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending 
                ? query.OrderBy(x => x.IntValue, OrderingType.Long) 
                : query.OrderByDescending(x => x.IntValue, OrderingType.Long);
        }
        var queryResults = await query.ToListAsync();

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
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDouble(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

        query = query.WhereExists(x => x.Id);
        if (fieldFirst)
        {
            query = isAscending ? query.OrderBy(x => x.DoubleValue, OrderingType.Double) : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending ? query.OrderBy(x => x.DoubleValue, OrderingType.Double) : query.OrderByDescending(x => x.DoubleValue, OrderingType.Double);
        }
        var queryResults = await query.ToListAsync();

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
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingAlphaNumeric(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Document> query = isAutoIndex
            ? session.Advanced.AsyncDocumentQuery<Document>()
            : session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>();

        query = query.WhereExists(x => x.Id);
        if (fieldFirst)
        {
            query = isAscending ? query.OrderBy(x => x.Name, OrderingType.AlphaNumeric) : query.OrderByDescending(x => x.Name, OrderingType.AlphaNumeric);
            query = query.OrderBy(x => x.ToIgnore);
        }
        else
        {
            query = query.OrderBy(x => x.ToIgnore);
            query = isAscending ? query.OrderBy(x => x.Name, OrderingType.AlphaNumeric) : query.OrderByDescending(x => x.Name, OrderingType.AlphaNumeric);
        }
        var queryResults = await query.ToListAsync();

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
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatial(Options options, bool nullFirst, bool isAutoIndex, bool isAscending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var orderClause = isAscending ? "" : " desc";
        string rql;

        if (fieldFirst)
        {
            rql = isAutoIndex
                ? $"from Documents where exists(ToIgnore) order by spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0)){orderClause}, ToIgnore"
                : $"from index '{new DocumentIndex().IndexName}' where exists(ToIgnore) order by spatial.distance(Location, spatial.point(0,0)){orderClause}, ToIgnore";
        }
        else
        {
            rql = isAutoIndex
                ? $"from Documents where exists(ToIgnore) order by ToIgnore, spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0)){orderClause}"
                : $"from index '{new DocumentIndex().IndexName}' where exists(ToIgnore) order by ToIgnore, spatial.distance(Location, spatial.point(0,0)){orderClause}";
        }

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();

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

    private async Task<DocumentStore> CreateDocumentsAndIndexes(Options options, bool nullFirst, bool autoIndex = false)
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
        var oneDocument = new Document { Name = "a", DoubleValue = 1, IntValue = 1, Location = new Geo { Latitude = 10, Longitude = 10 }, ToIgnore = nameof(Document.ToIgnore) };
        var twoDocument = new Document { Name = "b", DoubleValue = 2, IntValue = 2, Location = new Geo { Latitude = 20, Longitude = 20 }, ToIgnore = nameof(Document.ToIgnore) };
        var nonExistingFields = new Document { Name = null, DoubleValue = null, IntValue = null, Location = null, ToIgnore = nameof(Document.ToIgnore) };
        await session.StoreAsync(nullDocument);
        await session.StoreAsync(oneDocument);
        await session.StoreAsync(twoDocument);
        await session.StoreAsync(nonExistingFields);
        await session.SaveChangesAsync();

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

        if (autoIndex == false)
        {
            var index = new DocumentIndex();
            await index.ExecuteAsync(store);
        }
        else
        {
            // Create an autoindex
            var _ = await session.Query<Document>()
                .Statistics(out var stats)
                .Where(x => x.Name == "1" || x.DoubleValue > 0 || x.IntValue > 0 || x.ToIgnore == null)
                .Spatial(x => x.Point(doc => doc.Location.Latitude, doc => doc.Location.Longitude), f => f.WithinRadius(1000, 0, 0))
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
