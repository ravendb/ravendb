using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

using Raven.Imports.Newtonsoft.Json.Converters;
using Raven.Server.Json;
using Xunit;

namespace NewBlittable.Tests.BlittableJsonWriterTests
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
        public async Task FlatBoundarySizeFieldsAmount(int maxValue)
        {
            //var maxValue = short.MaxValue + 1000;
            var str = GetJsonString(maxValue);

            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = await blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
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

            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = await blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                var ms = new MemoryStream();
                employee.WriteTo(ms, originalPropertyOrder: true);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), str);
            }
        }
    }
}
