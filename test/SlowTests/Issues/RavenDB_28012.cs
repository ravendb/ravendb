using System;
using FastTests;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28012(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const int ByteActionTableSize = 256;

    [RavenFact(RavenTestCategory.Core)]
    public void WritingShortStringsAllocatesAsMuchAsAnEmptyArray()
    {
        string[] values = new string[200];
        for (int i = 0; i < values.Length; i++)
            values[i] = "Product " + i;

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context);

        long emptyArray = MinAllocatedBytes(builder, static (_, _) => { }, values);
        long shortStrings = MinAllocatedBytes(builder, static (b, strings) =>
        {
            foreach (string value in strings)
                b.WriteValue(value);
        }, values);

        Assert.True(shortStrings - emptyArray < ByteActionTableSize,
            $"Writing {values.Length} short strings allocated {shortStrings} bytes, an empty array allocated {emptyArray} bytes");
    }

    private static long MinAllocatedBytes(
        ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> builder,
        Action<ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>, string[]> writeItems,
        string[] values)
    {
        long min = long.MaxValue;
        for (int i = 0; i < 100; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();

            builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
            builder.StartWriteObjectDocument();
            builder.StartWriteObject();
            builder.WritePropertyName("Items");
            builder.StartWriteArray();
            writeItems(builder, values);
            builder.WriteArrayEnd();
            builder.WriteObjectEnd();
            builder.FinalizeDocument();
            using (builder.CreateReader())
            {
            }

            min = Math.Min(min, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        return min;
    }
}
