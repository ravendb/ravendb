using System;
using System.Text;
using Corax;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
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
        reader.Read(0, out long longValue);
        Assert.Equal(1, longValue);
        reader.ReadDynamic(Encoding.UTF8.GetBytes("Name_123"), out Span<byte> value);
        Assert.Equal("Oren", Encoding.UTF8.GetString(value));
        reader.ReadDynamic(Encoding.UTF8.GetBytes("Name_433"), out value);
        Assert.Equal("Eini", Encoding.UTF8.GetString(value));

        reader.ReadDynamic(Encoding.UTF8.GetBytes("Age_0"), out long lv);
        Assert.Equal(30, lv);
        reader.ReadDynamic(Encoding.UTF8.GetBytes("Age_0"), out double dl);
        Assert.Equal(30.31, dl);
        
        reader.ReadDynamic(Encoding.UTF8.GetBytes("Age_1"), out  lv);
        Assert.Equal(10, lv);
        reader.ReadDynamic(Encoding.UTF8.GetBytes("Age_1"), out  dl);
        Assert.Equal(10.55, dl);
    }
}
