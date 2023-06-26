using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using Assert = Xunit.Assert;
using FastTests.Voron;
using GeoAPI;
using NetTopologySuite;
using Sparrow;
using Sparrow.Server;
using Spatial4n.Context.Nts;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Sparrow.Threading;
using System.Xml.Linq;

namespace FastTests.Corax;

public class SpatialTests : StorageTest
{
    private const int IdIndex = 0, CoordinatesIndex = 1;
    private readonly IndexFieldsMapping _fieldsMapping;
    private static readonly NtsSpatialContext GeoContext;
    static SpatialTests()
    {
        GeometryServiceProvider.Instance = new NtsGeometryServices();
        GeoContext = new NtsSpatialContext(new NtsSpatialContextFactory { IsGeo = true });
    }
    
    
    public SpatialTests(ITestOutputHelper output) : base(output)
    {
        using var _ = StorageEnvironment.GetStaticContext(out var ctx);
        
        Slice.From(ctx, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(ctx, "Coordinates", ByteStringType.Immutable, out var idCoordinates);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice)
            .AddBinding(CoordinatesIndex, idCoordinates);
        _fieldsMapping = builder.Build();
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(48.666708, -4.333999, "gbsuv7s04")]
    [InlineData(53.015261, 18.611487, "u3mjxe0kr")]
    public void CanIndexReadAndDeleteLongLatSpatial(double latitude, double longitude, string geohash)
    {
        var IdString = "entry-1";
        var geohashes = Enumerable.Range(1, geohash.Length)
            .Select(i => 
                geohash.Substring(0, i)
                )
            .ToList();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, _fieldsMapping))
        {           
            var entry = new IndexEntryWriter(bsc, _fieldsMapping);
            entry.Write(IdIndex, Encodings.Utf8.GetBytes(IdString));
            var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
            entry.WriteSpatial(CoordinatesIndex, spatialEntry);
            using var _ = entry.Finish(out var preparedItem);
            writer.Index(IdString,preparedItem.ToSpan());
            writer.PrepareAndCommit();
        }

        for (int i = 0; i < geohash.Length; ++i)
        {
            var partialGeohash = geohash.Substring(0, i + 1);
            using (var searcher = new IndexSearcher(Env, _fieldsMapping))
            {
                Span<long> ids = new long[16];
                var entries = searcher.TermQuery("Coordinates", partialGeohash);
                Assert.Equal(1, entries.Fill(ids));

                var reader = searcher.GetEntryReaderFor(ids[0]);

                var fieldType = reader.GetFieldType(CoordinatesIndex, out int intOffset);
                Assert.Equal(IndexEntryFieldType.SpatialPoint, fieldType);
                
                reader.GetFieldReaderFor(CoordinatesIndex).Read(out (double, double) coords);
                Assert.Equal(coords.Item1, latitude);
                Assert.Equal(coords.Item2, longitude);
            }
        }

        using (var writer = new IndexWriter(Env, _fieldsMapping))
        {
            writer.TryDeleteEntry(IdString);
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, _fieldsMapping))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates", geohash);
            Assert.Equal(0, entries.Fill(ids));
        }
    }

    [Theory]
    [InlineData(4, new double[]{ -10.5, 12.4, -123D, 53}, new double[]{-52.123, 23.32123, 52.32423, -42.1235})]
    public unsafe void WriteAndReadSpatialList(int size, double[] lat, double[] lon)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        var entryBuilder = new IndexEntryWriter(bsc, _fieldsMapping);
        entryBuilder.Write(IdIndex, Encodings.Utf8.GetBytes("item/1"));
        Span<CoraxSpatialPointEntry> _points = new CoraxSpatialPointEntry[size];
        for (int i = 0; i < size; ++i)
            _points[i] = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
        entryBuilder.WriteSpatial(CoordinatesIndex, _points);
        using var _ = entryBuilder.Finish(out var buffer);
        var reader = new IndexEntryReader(buffer.Ptr, buffer.Length);

        Assert.True(reader.GetFieldType(CoordinatesIndex, out var intOffset).HasFlag(IndexEntryFieldType.SpatialPointList));
        var iterator = reader.ReadManySpatialPoint(CoordinatesIndex);
        List<CoraxSpatialPointEntry> entriesInIndex = new();
        
        while (iterator.ReadNext())
        {
            entriesInIndex.Add(iterator.CoraxSpatialPointEntry);
        }        
        
        Assert.Equal(size, entriesInIndex.Count);

        for (int i = 0; i < size; ++i)
        {
            var entry = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));

            var entryFromBuilder = entriesInIndex.Single(p => p.Geohash == entry.Geohash);
            Assert.Equal(entry.Latitude, entryFromBuilder.Latitude);
            Assert.Equal(entry.Longitude, entryFromBuilder.Longitude);
            entriesInIndex.Remove(entry);
        }
        
        Assert.Empty(entriesInIndex);
    }

    public override void Dispose()
    {
        base.Dispose();
        _fieldsMapping?.Dispose();
    }
}
