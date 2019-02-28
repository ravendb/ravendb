using System.IO;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12731 : NoDisposalNeeded
    {
        [Fact]
        public void CanCompareLazyStringValueAndLazyCompressedStringValue()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var ms = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, ms))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Test");
                writer.WriteString(new string('c', 1024 * 1024));
                writer.WriteEndObject();
                writer.Flush();
                ms.Flush();

                ms.Position = 0;
                var json = context.Read(ms, "test");

                ms.Position = 0;
                var json2 = context.Read(ms, "test");

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
