using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Compression;
using Sparrow.Json;
using Voron.Util;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class FunctionalityTests : BlittableJsonTestBase
    {
        [Fact]
        public void FunctionalityTest()
        {
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            var str = GenerateSimpleEntityForFunctionalityTest();
            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var employee =  blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
                dynamic dynamicBlittableJObject = new DynamicBlittableJson(employee);
                Assert.Equal(dynamicRavenJObject.Age, dynamicBlittableJObject.Age);
                Assert.Equal(dynamicRavenJObject.Name, dynamicBlittableJObject.Name);
                Assert.Equal(dynamicRavenJObject.Dogs.Count, dynamicBlittableJObject.Dogs.Count);
                for (var i = 0; i < dynamicBlittableJObject.Dogs.Length; i++)
                {
                    Assert.Equal(dynamicRavenJObject.Dogs[i], dynamicBlittableJObject.Dogs[i]);
                }
                Assert.Equal(dynamicRavenJObject.Office.Name, dynamicRavenJObject.Office.Name);
                Assert.Equal(dynamicRavenJObject.Office.Street, dynamicRavenJObject.Office.Street);
                Assert.Equal(dynamicRavenJObject.Office.City, dynamicRavenJObject.Office.City);
                var ms = new MemoryStream();
                blittableContext.Write(ms, employee);
                Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
            }
        }

        [Fact]
        public void FunctionalityTest2()
        {
            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                AssertComplexEmployee(str, employee, blittableContext);
            }
        }

        [Fact]
        public void EmptyArrayTest()
        {
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            var str = "{\"Alias\":\"Jimmy\",\"Data\":[],\"Name\":\"Trolo\",\"SubData\":{\"SubArray\":[]}}";
            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicObject = new DynamicBlittableJson(employee);
                Assert.Equal(dynamicObject.Alias, "Jimmy");
                Assert.Equal(dynamicObject.Data.Length, 0);
                Assert.Equal(dynamicObject.SubData.SubArray.Length, 0);
                Assert.Equal(dynamicObject.Name, "Trolo");
                Assert.Throws<IndexOutOfRangeException>(() => dynamicObject.Data[0]);
                Assert.Throws<IndexOutOfRangeException>(() => dynamicObject.SubData.SubArray[0]);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        [InlineData(10000)]
        public unsafe void LZ4Test(int size)
        {
            byte* encodeInput = (byte*)Marshal.AllocHGlobal(size);
            int compressedSize;
            byte* encodeOutput;
            using (var lz4 = new LZ4())
            {

                var originStr = string.Join("", Enumerable.Repeat(1, size).Select(x => "sample"));
                var bytes = Encoding.UTF8.GetBytes(originStr);
                fixed (byte* pb = bytes)
                {
                    var maximumOutputLength = LZ4.MaximumOutputLength(bytes.Length);
                    encodeOutput = (byte*)Marshal.AllocHGlobal(maximumOutputLength);
                    compressedSize = lz4.Encode64(pb, encodeOutput, bytes.Length, maximumOutputLength);
                }

                Array.Clear(bytes, 0, bytes.Length);
                fixed (byte* pb = bytes)
                {
                    LZ4.Decode64(encodeOutput, compressedSize, pb, bytes.Length, true);
                }
                var actual = Encoding.UTF8.GetString(bytes);
                Assert.Equal(originStr, actual);

                Marshal.FreeHGlobal((IntPtr)encodeInput);
                Marshal.FreeHGlobal((IntPtr)encodeOutput);
            }

        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        public void LongStringsTest(int repeatSize)
        {
            var originStr = string.Join("", Enumerable.Repeat(1, repeatSize).Select(x => "sample"));
            var sampleObject = new
            {
                SomeProperty = "text",
                SomeNumber = 1,
                SomeArray = new[] { 1, 2, 3 },
                SomeObject = new
                {
                    SomeValue = 1,
                    SomeArray = new[] { "a", "b" }
                },
                Value = originStr,
                AnotherNumber = 3
            };
            var str = sampleObject.ToJsonString();

            var unmanagedPool = new UnmanagedBuffersPool(string.Empty);

            using (var blittableContext = new JsonOperationContext(unmanagedPool))
            using (var doc =  blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicObject = new DynamicBlittableJson(doc);
                Assert.Equal(sampleObject.Value, dynamicObject.Value);
                Assert.Equal(sampleObject.SomeNumber, dynamicObject.SomeNumber);
                Assert.Equal(sampleObject.SomeArray.Length, dynamicObject.SomeArray.Length);
                Assert.Equal(sampleObject.SomeArray[0], dynamicObject.SomeArray[0]);
                Assert.Equal(sampleObject.AnotherNumber, dynamicObject.AnotherNumber);
                Assert.Equal(sampleObject.SomeArray[1], dynamicObject.SomeArray[1]);
                Assert.Equal(sampleObject.SomeArray[2], dynamicObject.SomeArray[2]);
                Assert.Equal(sampleObject.SomeObject.SomeValue, dynamicObject.SomeObject.SomeValue);
                Assert.Equal(sampleObject.SomeObject.SomeArray.Length, dynamicObject.SomeObject.SomeArray.Length);
                Assert.Equal(sampleObject.SomeObject.SomeArray[0], dynamicObject.SomeObject.SomeArray[0]);
                Assert.Equal(sampleObject.SomeObject.SomeArray[1], dynamicObject.SomeObject.SomeArray[1]);
                var ms = new MemoryStream();
                blittableContext.Write(ms, doc);
                Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
            }
        }

        [Fact]
        public void BasicCopyToStream()
        {
            var json = JsonConvert.SerializeObject(new
            {
                Name = "Oren",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                Age = "34",
                Office = new
                {
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            });

            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new JsonOperationContext(pool))
            using (var r =  ctx.Read(new MemoryStream(Encoding.UTF8.GetBytes(json)), "doc1"))
            {
                var ms = new MemoryStream();
                ctx.Write(ms, r);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), json);
            }
        }

        [Fact]
        public void UsingBoolleans()
        {
            var json = JsonConvert.SerializeObject(new
            {
                Name = "Oren",
                Dogs = true
            });

            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new JsonOperationContext(pool))
            using (var r = ctx.Read(new MemoryStream(Encoding.UTF8.GetBytes(json)), "doc1"))
            {
                var ms = new MemoryStream();
                ctx.Write(ms, r);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), json);
            }
        }

    }
}