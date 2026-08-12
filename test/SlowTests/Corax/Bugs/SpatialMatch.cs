using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using FastTests.Voron;
using Sparrow;
using Spatial4n.Context;
using Spatial4n.Util;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax.Bugs;

public class SpatialMatch(ITestOutputHelper output) : StorageTest(output)
{
    private const int AmericaHead = 100;
    private const int AustraliaCount = 3100;
    private const int EuropeCount = 3100;
    private const int AmericaTail = 100;
    private const int TotalDocuments = AmericaHead + AustraliaCount + EuropeCount + AmericaTail;
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(BitmapAndFillMode.Off)]
    [InlineData(BitmapAndFillMode.Auto)]
    [InlineData(BitmapAndFillMode.Force)]
    public void SpatialAndTermReturnsAllMatchingDocuments(BitmapAndFillMode mode)
    {
        using var mapping = CreateMapping();
        IndexDocuments(mapping);

        var spatialContext = SpatialContext.Geo;
        var circle = spatialContext.MakeCircle(0, 0, 175);

        using var searcher = new IndexSearcher(Env, mapping) { BitmapAndFillMode = mode };
        var expected = Drain(searcher.AllEntries());
        Assert.Equal(TotalDocuments, expected.Length);

        var and = searcher.And(
            SpatialQuery(searcher, mapping, circle, spatialContext),
            searcher.TermQuery("name", "common"));

        Assert.Equal(expected, Drain(and));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(BitmapAndFillMode.Off)]
    [InlineData(BitmapAndFillMode.Auto)]
    [InlineData(BitmapAndFillMode.Force)]
    public void TermOrSpatialReturnsAllMatchingDocuments(BitmapAndFillMode mode)
    {
        using var mapping = CreateMapping();
        IndexDocuments(mapping);

        var spatialContext = SpatialContext.Geo;
        var circle = spatialContext.MakeCircle(0, 0, 175);

        using var searcher = new IndexSearcher(Env, mapping) { BitmapAndFillMode = mode };
        var expected = Drain(searcher.AllEntries());
        var or = searcher.Or(
            searcher.TermQuery("region", "inner"),
            SpatialQuery(searcher, mapping, circle, spatialContext));

        Assert.Equal(expected, Drain(or));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(BitmapAndFillMode.Off)]
    [InlineData(BitmapAndFillMode.Auto)]
    [InlineData(BitmapAndFillMode.Force)]
    public void AndNotSpatialExcludesEverythingInsideTheShape(BitmapAndFillMode mode)
    {
        using var mapping = CreateMapping();
        IndexDocuments(mapping);

        var spatialContext = SpatialContext.Geo;
        var circle = spatialContext.MakeCircle(0, 0, 175);

        using var searcher = new IndexSearcher(Env, mapping) { BitmapAndFillMode = mode };

        var andNot = searcher.AndNot(
            searcher.AllEntries(),
            SpatialQuery(searcher, mapping, circle, spatialContext));

        Assert.Equal(Array.Empty<long>(), Drain(andNot));
    }

    private static IndexFieldsMapping CreateMapping() => IndexFieldsMappingBuilder.CreateForWriter(false)
        .AddBinding(0, "id()")
        .AddBinding(1, "name")
        .AddBinding(2, "region")
        .AddBinding(3, "coordinates")
        .Build();

    private void IndexDocuments(IndexFieldsMapping mapping)
    {
        var america = (Lat: 35.0, Lng: -110.0);
        var australia = (Lat: -25.0, Lng: 115.0);
        var europe = (Lat: 50.0, Lng: 10.0);

        var locations = new List<((double Lat, double Lng) Point, string Region)>();
        locations.AddRange(Enumerable.Repeat((america, "outer"), AmericaHead));
        locations.AddRange(Enumerable.Repeat((australia, "inner"), AustraliaCount));
        locations.AddRange(Enumerable.Repeat((europe, "inner"), EuropeCount));
        locations.AddRange(Enumerable.Repeat((america, "outer"), AmericaTail));

        using var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (int i = 0; i < locations.Count; i++)
        {
            var ((lat, lng), region) = locations[i];
            using var builder = writer.Index($"id/{i}");
            builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
            builder.Write(1, Encodings.Utf8.GetBytes("common"));
            builder.Write(2, Encodings.Utf8.GetBytes(region));
            builder.WriteSpatial(3, "coordinates",
                new CoraxSpatialPointEntry(lat, lng, GeohashUtils.EncodeLatLon(lat, lng, SpatialUtils.DefaultGeohashLevel)));
            builder.EndWriting();
        }

        writer.Commit();
    }

    private static IQueryMatch SpatialQuery(IndexSearcher searcher, IndexFieldsMapping mapping, Spatial4n.Shapes.IShape shape, SpatialContext spatialContext) =>
        searcher.SpatialQuery(mapping.GetByFieldId(3).Metadata, 0.025, shape, spatialContext, global::Corax.Utils.Spatial.SpatialRelation.Within);

    private static long[] Drain(IQueryMatch match)
    {
        var results = new List<long>();
        Span<long> buf = new long[8192];
        int read;
        while ((read = match.Fill(buf)) > 0)
        {
            for (int i = 0; i < read; i++)
                results.Add(buf[i]);
        }

        return results.Distinct().OrderBy(x => x).ToArray();
    }
}
