using System.IO;
using System.Threading.Tasks;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12731 : NoDisposalNeeded
    {
        public RavenDB_12731(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanCompareLazyStringValueAndLazyCompressedStringValue()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var ms = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Test");
                writer.WriteString(new string('c', 1024 * 1024));
                writer.WriteEndObject();
                await writer.FlushAsync();
                await ms.FlushAsync();

                ms.Position = 0;
                var json = await context.ReadForDiskAsync(ms, "test");

                ms.Position = 0;
                var json2 = await context.ReadForDiskAsync(ms, "test");

                var lcsv1 = (LazyCompressedStringValue)json["Test"];
                var lcsv2 = (LazyCompressedStringValue)json2["Test"];
                var lsv2 = lcsv2.ToLazyStringValue();

                Assert.Equal(lcsv1, lsv2);
                Assert.Equal(lsv2, lcsv1);
                Assert.Equal(lsv2, lcsv2);
                Assert.Equal(lcsv2, lsv2);
            }
        }
    }
}
