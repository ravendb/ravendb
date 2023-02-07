using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public unsafe class DynamicFieldsTests : StorageTest
{
    public DynamicFieldsTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void WriteEmptyAsSimpleInDynamicField()
    {
        const string fieldName = "Scope_0";
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);

        using var _ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        using var knownFields = builder.Build();
        IndexEntryWriter writer = new(bsc, knownFields);
        
        writer.WriteDynamic(fieldName, Encoding.UTF8.GetBytes(""));
        using var __ = writer.Finish(out ByteString element);
        IndexEntryReader reader = new(element.Ptr, element.Length);
        
        var fieldReader = reader.GetFieldReaderFor(Encoding.UTF8.GetBytes(fieldName));
        Assert.Equal(IndexEntryFieldType.Empty, fieldReader.Type);
    }

    [Fact]
    public void SimpleDynamicWrite()
    {
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);

        using IDisposable _ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        using IndexFieldsMapping knownFields = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice)
            .Build();

        IndexEntryWriter writer = new(bsc, knownFields);
        writer.Write(0, Encoding.UTF8.GetBytes("1.001"), 1, 1.001);
        writer.Write(1, new IndexEntryWriterTest.StringArrayIterator(new[] { "AAA", "BF", "CE" }));
        writer.WriteDynamic("Name_123", Encoding.UTF8.GetBytes("Oren"));
        writer.WriteDynamic("Name_433", Encoding.UTF8.GetBytes("Eini"));
        writer.WriteDynamic("Scope_0", Encoding.UTF8.GetBytes(""));
        writer.WriteNullDynamic("Scope_1");
        writer.WriteDynamic("Items_UK", new IndexEntryWriterTest.StringArrayIterator(new[] { "AAA", "GBP", "CE" }));

        writer.WriteDynamic("Age_0", Encoding.UTF8.GetBytes("30.31"), 30, 30.31);
        writer.WriteDynamic("Age_1", Encoding.UTF8.GetBytes("10"), 10, 10);

        using ByteStringContext<ByteStringMemoryCache>.InternalScope __ = writer.Finish(out ByteString element);

        IndexEntryReader reader = new(element.Ptr, element.Length);
        reader.GetFieldReaderFor(0).Read(out long longValue);
        Assert.Equal(1, longValue);
        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Name_123")).Read(out Span<byte> value);
        Assert.Equal("Oren", Encoding.UTF8.GetString(value));
        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Name_433")).Read(out value);
        Assert.Equal("Eini", Encoding.UTF8.GetString(value));

        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Age_0")).Read(out long lv);
        Assert.Equal(30, lv);
        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Age_0")).Read(out double dl);
        Assert.Equal(30.31, dl);
        
        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Age_1")).Read(out  lv);
        Assert.Equal(10, lv);
        reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Age_1")).Read(out  dl);
        Assert.Equal(10, dl);
        
        using (var indexer = new IndexWriter(Env, knownFields))
        {
            indexer.Index(IdString, element.ToSpan());
            indexer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.GreaterThanQuery(searcher.FieldMetadataBuilder("Age_1"), 5L);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Scope_0", Constants.EmptyString);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Scope_1", Constants.NullValue);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Items_UK", "GBP");
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("NotExists", Constants.EmptyString);
            Assert.Equal(0, entries.Fill(ids));
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(48.666708, -4.333999, "gbsuv7s04")]
    [InlineData(53.015261, 18.611487, "u3mjxe0kr")]
    public void CanIndexReadAndDeleteLongLatSpatialDynamically(double latitude, double longitude, string geohash)
    {
        using IndexFieldsMapping fields = PrepareSpatial();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, fields))
        {           
            var entry = new IndexEntryWriter(bsc, fields);
            entry.Write(0, Encodings.Utf8.GetBytes(IdString));
            var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
            entry.WriteSpatialDynamic("Coordinates_Home", spatialEntry);
            using var _ = entry.Finish(out var preparedItem);
            writer.Index(IdString, preparedItem.ToSpan());
            writer.Commit();
        }

        for (int i = 0; i < geohash.Length; ++i)
        {
            var partialGeohash = geohash.Substring(0, i + 1);
            using (var searcher = new IndexSearcher(Env, fields))
            {
                Span<long> ids = new long[16];
                var entries = searcher.TermQuery("Coordinates_Home", partialGeohash);
                Assert.Equal(1, entries.Fill(ids));

                var reader = searcher.GetEntryReaderFor(ids[0]);

                reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("Coordinates_Home")).Read(out (double, double) coords);
                Assert.Equal(coords.Item1, latitude);
                Assert.Equal(coords.Item2, longitude);
            }
        }

        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry("Id", IdString);
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates_Home", geohash);
            Assert.Equal(0, entries.Fill(ids));
        }
    }

    const string IdString = "entry-1";

    private static IndexFieldsMapping PrepareSpatial()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        return builder.Build();
    }

    [Theory]
    [InlineData(4, new double[]{ -10.5, 12.4, -123D, 53}, new double[]{-52.123, 23.32123, 52.32423, -42.1235})]
    public unsafe void WriteAndReadSpatialListDynamically(int size, double[] lat, double[] lon)
    {
        using IndexFieldsMapping fields = PrepareSpatial();
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        var entryBuilder = new IndexEntryWriter(bsc, fields);
        entryBuilder.Write(0, Encodings.Utf8.GetBytes("item/1"));
        Span<CoraxSpatialPointEntry> _points = new CoraxSpatialPointEntry[size];
        for (int i = 0; i < size; ++i)
            _points[i] = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
        entryBuilder.WriteSpatialDynamic("CoordinatesIndex", _points);
        using var _ = entryBuilder.Finish(out var buffer);

        var reader = new IndexEntryReader(buffer.Ptr, buffer.Length);

        var fieldReader = reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("CoordinatesIndex"));

        Assert.True(fieldReader.TryReadManySpatialPoint(out SpatialPointFieldIterator iterator));
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
}
