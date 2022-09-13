using System;
using System.Linq;
using System.Text;
using Corax;
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

public class DynamicFieldsTests : StorageTest
{
    public DynamicFieldsTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void SimpleDynamicWrite()
    {
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);

        using IDisposable _ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        IndexFieldsMapping knownFields = new IndexFieldsMapping(ctx)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);

        IndexEntryWriter writer = new(bsc, knownFields);
        writer.Write(0, Encoding.UTF8.GetBytes("1.001"), 1, 1.001);
        writer.Write(1, new IndexEntryWriterTest.StringArrayIterator(new[] { "AAA", "BF", "CE" }));
        writer.WriteDynamic("Name_123", Encoding.UTF8.GetBytes("Oren"));
        writer.WriteDynamic("Name_433", Encoding.UTF8.GetBytes("Eini"));
        
        writer.WriteDynamic("Age_0", Encoding.UTF8.GetBytes("30.31"), 30, 30.31);
        writer.WriteDynamic("Age_1", Encoding.UTF8.GetBytes("10.55"), 10, 10.55);

        using ByteStringContext<ByteStringMemoryCache>.InternalScope __ = writer.Finish(out ByteString element);

        IndexEntryReader reader = new(element.ToSpan());
        reader.GetReaderFor(0).Read(out long longValue);
        Assert.Equal(1, longValue);
        reader.GetReaderFor(Encoding.UTF8.GetBytes("Name_123")).Read(out Span<byte> value);
        Assert.Equal("Oren", Encoding.UTF8.GetString(value));
        reader.GetReaderFor(Encoding.UTF8.GetBytes("Name_433")).Read(out value);
        Assert.Equal("Eini", Encoding.UTF8.GetString(value));

        reader.GetReaderFor(Encoding.UTF8.GetBytes("Age_0")).Read(out long lv);
        Assert.Equal(30, lv);
        reader.GetReaderFor(Encoding.UTF8.GetBytes("Age_0")).Read(out double dl);
        Assert.Equal(30.31, dl);
        
        reader.GetReaderFor(Encoding.UTF8.GetBytes("Age_1")).Read(out  lv);
        Assert.Equal(10, lv);
        reader.GetReaderFor(Encoding.UTF8.GetBytes("Age_1")).Read(out  dl);
        Assert.Equal(10.55, dl);
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
        
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        IndexFieldsMapping fields = new IndexFieldsMapping(ctx)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);


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

                var reader = searcher.GetReaderFor(ids[0]);

                var fieldType = reader.GetFieldType(Encoding.UTF8.GetBytes("Coordinates_Home"), out int intOffset);
                Assert.Equal(IndexEntryFieldType.SpatialPoint, fieldType);
                
                reader.GetReaderFor(Encoding.UTF8.GetBytes("Coordinates_Home")).Read(out (double, double) coords);
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
}
