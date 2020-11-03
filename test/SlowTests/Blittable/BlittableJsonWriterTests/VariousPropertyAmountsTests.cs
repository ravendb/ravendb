using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Blittable.BlittableJsonWriterTests
{
    public class VariousPropertyAmountsTests : NoDisposalNeeded
    {
        public VariousPropertyAmountsTests(ITestOutputHelper output) : base(output)
        {
        }

        public string GetJsonString(int size)
        {
            var sb = new StringBuilder();
            sb.Append("{");

            for (int i = 0; i < size; i++)
            {
                if (i != 0)
                    sb.Append(",");
                sb.Append("\"Field").Append(i).Append("\":").Append(i);
            }
            sb.Append("}");

            return sb.ToString();
        }

        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public void FlatBoundarySizeFieldsAmount(int maxValue)
        {
            //var maxValue = short.MaxValue + 1000;
            var str = GetJsonString(maxValue);

            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicBlittableJObject = new DynamicBlittableJson(employee);

                for (var i = 0; i < maxValue; i++)
                {
                    string key = "Field" + i;
                    long curVal = dynamicBlittableJObject[key];
                    Assert.Equal(curVal, i);
                }
            }
        }

        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public async Task FlatBoundarySizeFieldsAmountStreamRead(int maxValue)
        {
            var str = GetJsonString(maxValue);

            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                var ms = new MemoryStream();
                await blittableContext.WriteAsync(ms, employee);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), str);
            }
        }
    }
}
