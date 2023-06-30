using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class IncludeScoresAndDistancesCorax(ITestOutputHelper output) : RavenTestBase(output)
{
    private string[] _data = {"maciej", "gracjan", "michal", "arek", "pawel"};
    private Random _random = new(123124);

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [InlineData(true)]
    [InlineData(false)]
    public void CanGetScoreInMetadata(bool compoundSorting)
    {
        using var store = PrepareData();
        using var session = store.OpenSession();

        var query = session.Advanced.DocumentQuery<StringAndSpatial, SpatialIndex>()
            .Search(x => x.Name, string.Join(" ", _data))
            .OrderByScore();

        if (compoundSorting)
            query = query.OrderBy(x => x.Lat);

        IEnumerator<StreamResult<StringAndSpatial>> streamResults = session.Advanced.Stream(query, out StreamQueryStatistics streamQueryStats);
        while (streamResults.MoveNext())
        {
            Assert.NotNull(streamResults.Current);
            Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [InlineData(true)]
    [InlineData(false)]
    public void CanGetSpatialResults(bool compoundSorting)
    {
        using var store = PrepareData();
        using var session = store.OpenSession();

        var query = session.Advanced.DocumentQuery<StringAndSpatial, SpatialIndex>()
            .Search(x => x.Name, string.Join(" ", _data))
            .OrderByDistance(s => s.SpatialField, 0, 0);

        if (compoundSorting)
            query = query.OrderBy(x => x.Lat);

        IEnumerator<StreamResult<StringAndSpatial>> streamResults = session.Advanced.Stream(query, out StreamQueryStatistics streamQueryStats);
        while (streamResults.MoveNext())
        {
            Assert.NotNull(streamResults.Current);
            var spatialResult = (IDictionary<string, object>)streamResults.Current.Metadata[Constants.Documents.Metadata.SpatialResult];
            Assert.NotNull(spatialResult);
            Assert.Equal(streamResults.Current.Document.Lat, (double)spatialResult["Latitude"], precision: 5);
            Assert.Equal(streamResults.Current.Document.Lng, (double)spatialResult["Longitude"], precision: 5);
        }
    }

    [Fact]
    public void DefaultBufferSizeForCoraxHasNotChanged()
    {
        // The tests in this file were designed for a pageSize of 4096. In case the default buffer has changed, please update the number of documents above that limit.
        // These tests need to call .Fill at least twice to ensure that distances/scores can be transferred from Corax to Raven in a streaming manner.

        Type type = typeof(Raven.Server.Documents.Indexes.Persistence.IndexOperationBase);
        FieldInfo field = type.GetField("DefaultBufferSizeForCorax",
            BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        Assert.NotNull(field);
        Assert.True(field.IsLiteral);
        var coraxDefaultPageSize = (int)field.GetValue(null);
        Assert.Equal(4096, coraxDefaultPageSize);
    }

    private IDocumentStore PrepareData()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeSpatialDistance)] = true.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            for (int idX = 0; idX < 5_000; ++idX)
            {
                var dto = new StringAndSpatial(_data[_random.Next(_data.Length)], _random.NextDouble() * 40 + 0.1d, _random.NextDouble() * 40 + 0.1d);
                bulk.Store(dto);
            }
        }

        new SpatialIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        return store;
    }

    private record StringAndSpatial(string Name, double Lat, double Lng, string Id = null, object SpatialField = null);

    private class SpatialIndex : AbstractIndexCreationTask<StringAndSpatial>
    {
        public SpatialIndex()
        {
            Map = spatials => spatials.Select(x => new {SpatialField = CreateSpatialField(x.Lat, x.Lng), Name = x.Name, Lat = x.Lat});
            Index(x => x.Name, FieldIndexing.Search);
        }
    }
}
