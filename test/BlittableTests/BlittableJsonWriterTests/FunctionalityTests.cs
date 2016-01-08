using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using NewBlittable.Tests;
using Newtonsoft.Json;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Server.Json;
using Voron.Util;
using Xunit;
using JsonTextWriter = Raven.Imports.Newtonsoft.Json.JsonTextWriter;


namespace BlittableTests.BlittableJsonWriterTests
{
    public unsafe class FunctionalityTests : BlittableJsonTestBase
    {
        [Fact]
        public void FunctionalityTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024*1024*1024);

            var str = GenerateSimpleEntityForFunctionalityTest();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new JsonTextReader(new StringReader(str)),
                "doc1"))
            {
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
                dynamic dynamicBlittableJObject = new DynamicBlittableJson(ptr, employee.SizeInBytes, blittableContext);
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
                new BlittableJsonReaderObject(ptr, employee.SizeInBytes,blittableContext).WriteTo(ms);
                Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
            }
        }

        [Fact]
        public void FunctionalityTest2()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024*1024*1024);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new JsonTextReader(new StringReader(str)),
                "doc1"))
            {
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                AssertComplexEmployee(str, ptr, employee.SizeInBytes, blittableContext);
            }
        }

        [Fact]
        public void EmptyArrayTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024*1024*1024);

            var str = "{\"Alias\":\"Jimmy\",\"Data\":[],\"Name\":\"Trolo\",\"SubData\":{\"SubArray\":[]}}";
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new JsonTextReader(new StringReader(str)),
                "doc1"))
            {
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                dynamic dynamicObject = new DynamicBlittableJson(ptr, employee.SizeInBytes, blittableContext);
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
        public void LZ4Test(int size)
        {
            byte* encodeInput = (byte*) Marshal.AllocHGlobal(size);
            int compressedSize;
            byte* encodeOutput;
            var lz4 = new LZ4();

            var originStr = string.Join("", Enumerable.Repeat(1, size).Select(x => "sample"));
            var bytes = Encoding.UTF8.GetBytes(originStr);
            fixed (byte* pb = bytes)
            {
                var maximumOutputLength = LZ4.MaximumOutputLength(bytes.Length);
                encodeOutput = (byte*) Marshal.AllocHGlobal(maximumOutputLength);
                compressedSize = lz4.Encode64(pb, encodeOutput, bytes.Length, maximumOutputLength);
            }

            Array.Clear(bytes, 0, bytes.Length);
            fixed (byte* pb = bytes)
            {
                LZ4.Decode64(encodeOutput, compressedSize, pb, bytes.Length, true);
            }
            var actual = Encoding.UTF8.GetString(bytes);
            Assert.Equal(originStr, actual);

            Marshal.FreeHGlobal((IntPtr) encodeInput);
            Marshal.FreeHGlobal((IntPtr) encodeOutput);
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
                SomeArray = new[] {1, 2, 3},
                SomeObject = new
                {
                    SomeValue = 1,
                    SomeArray = new[] {"a", "b"}
                },
                Value = originStr,
                AnotherNumber = 3
            };
            var str = sampleObject.ToJsonString();

            byte* ptr;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024*1024*1024);

            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = blittableContext.Read(new JsonTextReader(new StringReader(str)), 
                "doc1"))
            {
                int size;
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                dynamic dynamicObject = new DynamicBlittableJson(ptr, employee.SizeInBytes, blittableContext);
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
                new BlittableJsonReaderObject(ptr, employee.SizeInBytes, blittableContext).WriteTo(ms);
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

            using (var pool = new UnmanagedBuffersPool("test", 1024 * 1024))
            using (var ctx = new RavenOperationContext(pool))
            using (var obj = ctx.Read(new JsonTextReader(new StringReader(json)), "test/1"))
            {
                int size;
                var buffer = ctx.GetNativeTempBuffer(obj.SizeInBytes, out size);
                size = obj.CopyTo(buffer);
                var r = new BlittableJsonReaderObject(buffer, size, ctx);

                var ms = new MemoryStream();
                r.WriteTo(ms);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), json);
            }
        }


    }
}