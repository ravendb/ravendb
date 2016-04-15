using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class VariousPropertyAmountsTests
    {
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

            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
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
        public void FlatBoundarySizeFieldsAmountStreamRead(int maxValue)
        {

            var str = GetJsonString(maxValue);

            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                var ms = new MemoryStream();
                blittableContext.Write(ms, employee);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), str);
            }
        }
    }
}
