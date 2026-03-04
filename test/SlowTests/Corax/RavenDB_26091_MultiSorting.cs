using System;
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

namespace SlowTests.Corax;

public class RavenDB_26091_MultiSorting(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingStringBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingStringBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.Name)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderBy(x => x.Name)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingIntBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingIntBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.IntValue, OrderingType.Long)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderBy(x => x.IntValue, OrderingType.Long)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.DoubleValue, OrderingType.Double)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderBy(x => x.DoubleValue, OrderingType.Double)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringDescendingAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingStringDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringDescendingStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingStringDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingStringDescendingBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderByDescending(x => x.Name)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderByDescending(x => x.Name)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntDescendingAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingIntDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntDescendingStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingIntDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingIntDescendingBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderByDescending(x => x.IntValue, OrderingType.Long)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderByDescending(x => x.IntValue, OrderingType.Long)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleDescendingAutoIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document>());

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleDescendingStaticIndex(Options options, bool nullFirst, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleDescendingBase(options, nullFirst, fieldFirst, session => session.Advanced.AsyncDocumentQuery<Document, DocumentIndex>());

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingDoubleDescendingBase(Options options, bool nullFirst, bool fieldFirst, Func<IAsyncDocumentSession, IAsyncDocumentQuery<Document>> queryCreator)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst);
        using var session = store.OpenAsyncSession();

        var queryResults = fieldFirst
            ? await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
                .OrderBy(x => x.ToIgnore)
                .ToListAsync()
            : await queryCreator(session)
                .WhereExists(x => x.Id)
                .OrderBy(x => x.ToIgnore)
                .OrderByDescending(x => x.DoubleValue, OrderingType.Double)
                .ToListAsync();

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatialAutoIndex(Options options, bool nullFirst, bool ascending, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatialBase(options, nullFirst, autoIndex: true, ascending, fieldFirst);

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true, false, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, true, false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false, false, false])]
    public async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatialStaticIndex(Options options, bool nullFirst, bool ascending, bool fieldFirst) => await
        CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatialBase(options, nullFirst, autoIndex: false, ascending, fieldFirst);

    private async Task CanChangeOrderOfTheNullsWhenMultiFieldSortingSpatialBase(Options options, bool nullFirst, bool autoIndex, bool ascending, bool fieldFirst)
    {
        using var store = await CreateDocumentsAndIndexes(options, nullFirst, autoIndex);
        using var session = store.OpenAsyncSession();

        var orderClause = ascending ? "" : " desc";
        string rql;

        if (fieldFirst)
        {
            rql = autoIndex
                ? $"from Documents where exists(ToIgnore) order by spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0)){orderClause}, ToIgnore"
                : $"from index '{new DocumentIndex().IndexName}' where exists(ToIgnore) order by spatial.distance(Location, spatial.point(0,0)){orderClause}, ToIgnore";
        }
        else
        {
            rql = autoIndex
                ? $"from Documents where exists(ToIgnore) order by ToIgnore, spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0,0)){orderClause}"
                : $"from index '{new DocumentIndex().IndexName}' where exists(ToIgnore) order by ToIgnore, spatial.distance(Location, spatial.point(0,0)){orderClause}";
        }

        var queryResults = await session.Advanced.AsyncRawQuery<Document>(rql).ToListAsync();

        Assert.Equal(4, queryResults.Count);

        switch (ascending, nullFirst)
        {
            case (ascending: true, nullFirst: true):
                Assert.Null(queryResults[0].Location);
                Assert.Null(queryResults[1].Location);
                Assert.Equal(expected: 10, actual: queryResults[2].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[2].Location.Longitude);
                Assert.Equal(expected: 20, actual: queryResults[3].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[3].Location.Longitude);
                break;

            case (ascending: true, nullFirst: false):
                Assert.Equal(expected: 10, actual: queryResults[0].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[0].Location.Longitude);
                Assert.Equal(expected: 20, actual: queryResults[1].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[1].Location.Longitude);
                Assert.Null(queryResults[2].Location);
                Assert.Null(queryResults[3].Location);
                break;

            case (ascending: false, nullFirst: true):
                Assert.Equal(expected: 20, actual: queryResults[0].Location.Latitude);
                Assert.Equal(expected: 20, actual: queryResults[0].Location.Longitude);
                Assert.Equal(expected: 10, actual: queryResults[1].Location.Latitude);
                Assert.Equal(expected: 10, actual: queryResults[1].Location.Longitude);
                Assert.Null(queryResults[2].Location);
                Assert.Null(queryResults[3].Location);
                break;

            case (ascending: false, nullFirst: false):
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
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.NullFirst)] = nullFirst.ToString();
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
