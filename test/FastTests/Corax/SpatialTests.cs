using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Corax;
using Corax.Utils;
using Assert = Xunit.Assert;
using FastTests.Voron;
using GeoAPI;
using NetTopologySuite;
using Sparrow;
using Sparrow.Server;
using Spatial4n.Core.Context;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.IO.Nts;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Impl;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using SpatialRelation = Corax.Utils.SpatialRelation;

namespace FastTests.Corax;

public class SpatialTests : StorageTest
{
    private const int IdIndex = 0, CoordinatesIndex = 1;
    private readonly IndexFieldsMapping _fieldsMapping;
    private static readonly NtsSpatialContext GeoContext;
    static SpatialTests()
    {
        GeometryServiceProvider.Instance = new NtsGeometryServices();
        GeoContext = new NtsSpatialContext(new NtsSpatialContextFactory { geo = true });
    }
    
    
    public SpatialTests(ITestOutputHelper output) : base(output)
    {
        using var _ = StorageEnvironment.GetStaticContext(out var ctx);

        _fieldsMapping = new (ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(ctx, "Coordinates", ByteStringType.Immutable, out var idCoordinates);

        _fieldsMapping.AddBinding(IdIndex, idSlice);
        _fieldsMapping.AddBinding(CoordinatesIndex, idCoordinates);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(48.666708, -4.333999, "gbsuv7s04")]
    [InlineData(53.015261, 18.611487, "u3mjxe0kr")]
    public void CanIndexReadAndDeleteLongLatSpatial(double latitude, double longitude, string geohash)
    {
        var IdString = "entry-1";
        var geohashesh = Enumerable.Range(1, geohash.Length)
            .Select(i => 
                geohash.Substring(0, i)
                )
            .ToList();

        using (var writer = new IndexWriter(Env, _fieldsMapping))
        {
            Span<byte> buffer = new byte[1024 * 16];
            var entry = new IndexEntryWriter(buffer, _fieldsMapping);
            entry.Write(IdIndex, Encodings.Utf8.GetBytes(IdString));
            var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
            entry.WriteSpatial(CoordinatesIndex, spatialEntry);
            entry.Finish(out var preparedItem);
            writer.Index(IdString, preparedItem, _fieldsMapping);
            writer.Commit();
        }

        for (int i = 0; i < geohash.Length; ++i)
        {
            var partialGeohash = geohash.Substring(0, i + 1);
            using (var searcher = new IndexSearcher(Env, _fieldsMapping))
            {
                Span<long> ids = new long[16];
                var entries = searcher.TermQuery("Coordinates", partialGeohash, CoordinatesIndex);
                Assert.Equal(1, entries.Fill(ids));

                var reader = searcher.GetReaderFor(ids[0]);

                var fieldType = reader.GetFieldType(CoordinatesIndex, out int intOffset);
                Assert.Equal(IndexEntryFieldType.Extended, fieldType);

                var specialFieldType = reader.GetSpecialFieldType(ref intOffset);
                Assert.Equal(ExtendedEntryFieldType.SpatialPoint, specialFieldType);

                reader.Read(CoordinatesIndex, out (double, double) coords);
                Assert.Equal(coords.Item1, latitude);
                Assert.Equal(coords.Item2, longitude);
            }
        }

        using (var writer = new IndexWriter(Env, _fieldsMapping))
        {
            writer.TryDeleteEntry("Id", IdString);
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, _fieldsMapping))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates", geohash, CoordinatesIndex);
            Assert.Equal(0, entries.Fill(ids));
        }
    }

    [Theory]
    [InlineData(4, new double[]{ -10.5, 12.4, -123D, 53}, new double[]{-52.123, 23.32123, 52.32423, -42.1235})]
    public unsafe void WriteAndReadSpatialList(int size, double[] lat, double[] lon)
    {
        Span<byte> buffer = new byte[2048];
        var entryBuilder = new IndexEntryWriter(buffer, _fieldsMapping);
        entryBuilder.Write(IdIndex, Encodings.Utf8.GetBytes("item/1"));
        Span<CoraxSpatialPointEntry> _points = new CoraxSpatialPointEntry[size];
        for (int i = 0; i < size; ++i)
            _points[i] = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
        entryBuilder.WriteSpatial(CoordinatesIndex, _points);

        entryBuilder.Finish(out buffer);

        var reader = new IndexEntryReader(buffer);

        Assert.True(reader.GetFieldType(CoordinatesIndex, out var intOffset).HasFlag(IndexEntryFieldType.ExtendedList));
        Assert.True(reader.GetSpecialFieldType(ref intOffset).HasFlag(ExtendedEntryFieldType.SpatialPoint));
        var iterator = reader.ReadManySpatialPoint(CoordinatesIndex);
        List<CoraxSpatialPointEntry> entriesInIndex = new();
        
        while (iterator.ReadNext())
        {
            entriesInIndex.Add(iterator.CoraxSpatialPointEntry);
        }
        
        
        Assert.Equal(size, entriesInIndex.Count);

        for (int i = 0; i < size; ++i)
        {
            var entry = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));

            var entryFromBuilder = entriesInIndex.Single(p => p.Geohash == entry.Geohash);
            Assert.Equal(entry.Latitude, entryFromBuilder.Latitude);
            Assert.Equal(entry.Longitude, entryFromBuilder.Longitude);
            entriesInIndex.Remove(entry);
        }
        
        Assert.Empty(entriesInIndex);
    }

    [Fact]
    public void SpatialUtils()
    {
        var results = new List<string>();
        var toCheckManually = new List<string>();

        IShape circle;
        for (var i = 0; i < 1; ++i)
        {
            circle = new Circle(new Point(10, 10, SpatialContext.GEO), .2, SpatialContext.GEO);
            //global::Corax.Utils.SpatialHelper.GeohasheshOfShape(Spatial4n.Context.SpatialContext.GEO, circle, 1, 12, new ());
            global::Corax.Utils.SpatialHelper.FulfillShape(SpatialContext.GEO, circle, results,toCheckManually, maxPrecision: 6);

        }
        // IShape poland = GeoContext.ReadShape(
    //     "Polygon((14.260253906249998 53.969012350740314,14.47998046875 53.18628757391329,14.172363281250002 52.89564866211353,14.58984375 52.58970076871779,14.809570312500002 51.944264879028765,14.611816406249998 51.76783988732214,15.095214843750004 51.19311524464587,14.853515625 50.972264889367494,15.512695312499998 50.805934726769095,16.3037109375 50.6947178381929,16.413574218749996 50.54136296522162,16.127929687499996 50.373496144303516,16.6552734375 50.13466432216697,17.028808593749996 50.26125382758474,16.875000000000004 50.45750402042057,17.709960937499996 50.33143633083884,17.9736328125 50.035973672195496,18.632812499999996 49.88047763874255,18.962402343749996 49.43955695894084,19.379882812500004 49.582226044621706,19.929199218750004 49.15296965617043,20.500488281249996 49.38237278700956,20.895996093750004 49.339440937155445,21.730957031249996 49.39667507519394,22.21435546875 49.224772722794825,22.763671875 49.03786794532644,22.631835937499996 49.53946900793534,23.73046875 50.373496144303516,24.06005859375 50.45750402042057,24.0380859375 50.736455137010665,23.686523437500004 51.57706953722564,23.642578125 52.052490476001,23.291015625000004 52.2412561496634,23.75244140625 52.66972038368817,23.994140625 52.81604319154934,23.576660156249996 54.020679551599954,23.225097656249996 54.27805485967281,22.873535156249996 54.38055736863063,19.929199218750004 54.39335222384588,19.204101562499996 54.36775852406839,18.566894531249996 54.457266680933856,18.413085937500004 54.72462019492448,18.918457031249996 54.57206165565853,18.5888671875 54.76267040025496,17.841796875 54.85131525968606,16.58935546875 54.59752785211387,15.996093749999996 54.2267077643867,14.260253906249998 53.969012350740314))");
    //     
    //global::Corax.Utils.SpatialHelper.FulfillShape(Spatial4n.Context.SpatialContext.GEO, circle, results);
        var sb = new StringBuilder();
        sb.Append('[');
        foreach (var geo in results)
        {
            sb.Append($",'{geo}'");
        }
        
        sb.Append(']');
        sb.Remove(1, 1);
        var res = sb.ToString();
        File.WriteAllText("C:\\workspace\\geo\\out.txt", res);
    }


    // [Fact]
    // public void CircleTest()
    // {
    //     using (var writer = new IndexWriter(Env, _fieldsMapping))
    //     {
    //         Span<byte> buffer = new byte[1024 * 16];
    //         var entry = new IndexEntryWriter(buffer, _fieldsMapping);
    //         
    //         entry.Write(IdIndex, Encodings.Utf8.GetBytes("entry-1"));
    //         entry.WriteSpatial(CoordinatesIndex, 10.05, 10.07, new FastTests.Corax.IndexEntryWriterTest.StringArrayIterator(new []{ Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(10.5, 10.07, 12) }));
    //         entry.Finish(out var preparedItem);
    //         writer.Index("entry-1", preparedItem, _fieldsMapping);
    //         
    //         entry = new IndexEntryWriter(buffer, _fieldsMapping);
    //         entry.Write(IdIndex, Encodings.Utf8.GetBytes("entry-2"));
    //         entry.WriteSpatial(CoordinatesIndex, -10.05, 10.07, new FastTests.Corax.IndexEntryWriterTest.StringArrayIterator(new []{ Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(-10.5, 10.07, 12) }));
    //         entry.Finish(out preparedItem);
    //         writer.Index("entry-2", preparedItem, _fieldsMapping);
    //         
    //         writer.Commit();
    //     }
    //
    //     using (var searcher = new IndexSearcher(Env, _fieldsMapping))
    //     {
    //         IShape circle = new Circle(new Point(10, 10, SpatialContext.GEO), 0.2, SpatialContext.GEO);
    //         var match = searcher.SpatialQuery("Coordinates", CoordinatesIndex, Double.Epsilon, circle, SpatialRelation.Within);
    //
    //         Span<long> ids = new long[16];
    //         var read = match.Fill(ids);
    //         Assert.Equal(1, read);
    //         
    //     }
    // }
}
