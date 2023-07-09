using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_19283 : StorageTest
{
    public RavenDB_19283(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public unsafe void CanReadAndWriteLargeEntries()
    {
        Assert.Fail("Fix me");
        // using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        //
        // using var _ = StorageEnvironment.GetStaticContext(out var ctx);
        // Slice.From(ctx, "Items", ByteStringType.Immutable, out Slice itemsSlice);
        // Slice.From(ctx, "id()", ByteStringType.Immutable, out Slice idSlice);
        //
        // // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        //
        // using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
        //     .AddBinding(0, idSlice)
        //     .AddBinding(1, itemsSlice);
        // using var knownFields = builder.Build();
        //
        // var options = new[] { "one", "two", "three" };
        //
        // var writer = new IndexEntryWriter(bsc, knownFields);
        // var tags = Enumerable.Range(0, 10000).Select(x => options[x % options.Length]);
        // writer.Write(1, new IndexEntryWriterTest.StringArrayIterator(tags.ToArray()));
        // writer.Write(0, Encoding.UTF8.GetBytes("users/1"));
        // using var ___ = writer.Finish(out var element);
        //
        // var reader = new IndexEntryReader(element.Ptr, element.Length);
        // reader.GetFieldReaderFor(0).Read(out Span<byte> id);
        // var it = reader.GetFieldReaderFor(1).ReadMany();
        // while (it.ReadNext())
        // {
        // }
    }
}
