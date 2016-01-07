using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using ConsoleApplication4;
using NewBlittable;
using NewBlittable.Tests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Client.Document;
using Raven.Json.Linq;
//using Raven.Imports.Newtonsoft.Json;
//using Raven.Json.Linq;
using Raven.Server.Json;
using Voron.Util;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new FunctionalityTests().FunctionalityTest();

        }
    }

    public class BlittableJsonTestBase
    {
        public string GenerateSimpleEntityForFunctionalityTest()
        {
            object employee = new
            {
                Name = "Oren",
                Age = "34",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                Office = new
                {
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            };

            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, employee);

            return stringWriter.ToString();
        }

        public string GenerateSimpleEntityForFunctionalityTest2()
        {
            object employee = new
            {
                Name = "Oren",
                Age = "34",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                MegaDevices = new[]
                {
                  new
                  {
                      Name = "Misteryous Brain Disruptor",
                      Usages = 0
                  }  ,
                  new
                  {
                      Name="Hockey stick",
                      Usages = 4
                  }
                },
                Office = new
                {
                    Manager = new
                    {
                        Name = "Assi",
                        Id = 44
                    },
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            };

            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, employee);

            return stringWriter.ToString();
        }

        protected static unsafe void AssertComplexEmployee(string str, byte* ptr, BlittableJsonWriter employee,
         RavenOperationContext blittableContext)
        {
            dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
            dynamic dynamicBlittableJObject = new DynamicBlittableJson(ptr, employee.SizeInBytes,
                blittableContext);

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
            Assert.Equal(dynamicRavenJObject.Office.Manager.Name, dynamicRavenJObject.Office.Manager.Name);
            Assert.Equal(dynamicRavenJObject.Office.Manager.Id, dynamicRavenJObject.Office.Manager.Id);

            Assert.Equal(dynamicRavenJObject.MegaDevices.Count, dynamicBlittableJObject.MegaDevices.Count);
            for (var i = 0; i < dynamicBlittableJObject.MegaDevices.Length; i++)
            {
                Assert.Equal(dynamicRavenJObject.MegaDevices[i].Name,
                    dynamicBlittableJObject.MegaDevices[i].Name);
                Assert.Equal(dynamicRavenJObject.MegaDevices[i].Usages,
                    dynamicBlittableJObject.MegaDevices[i].Usages);
            }
        }
    }


    public unsafe class FunctionalityTests : BlittableJsonTestBase
    {
        [Fact]
        public void FunctionalityTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = GenerateSimpleEntityForFunctionalityTest();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                MemoryStream stream = new MemoryStream();
                BlittableJsonReaderObject reader = new BlittableJsonReaderObject(ptr, employee.SizeInBytes, blittableContext);
                reader.WriteObjectAsJsonStringAsync(stream).Wait();

                var byteArray = stream.ToArray();
                var unicodeEncoding = new UnicodeEncoding();
                //var charCount = unicodeEncoding.GetCharCount(byteArray, 0, byteArray.Length);
                var chars = unicodeEncoding.GetChars(byteArray);
                var strsss = new string(chars);
                var trolo = strsss;


                //WriteObjectAsJsonStringAsync
                /*dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
                dynamic dynamicBlittableJObject = new DynamicBlittableJson(ptr, employee.SizeInBytes, blittableContext);
                Assert.Equal(dynamicRavenJObject.Age, (string)dynamicBlittableJObject.Age);
                Assert.Equal(dynamicRavenJObject.Name, (string)dynamicBlittableJObject.Name);
                Assert.Equal(dynamicRavenJObject.Dogs.Count, dynamicBlittableJObject.Dogs.Count);
                for (var i = 0; i < dynamicBlittableJObject.Dogs.Length; i++)
                {
                    Assert.Equal(dynamicRavenJObject.Dogs[i], (string)dynamicBlittableJObject.Dogs[i]);
                }
                Assert.Equal(dynamicRavenJObject.Office.Name, (string)dynamicRavenJObject.Office.Name);
                Assert.Equal(dynamicRavenJObject.Office.Street, (string)dynamicRavenJObject.Office.Street);
                Assert.Equal(dynamicRavenJObject.Office.City, (string)dynamicRavenJObject.Office.City);*/
            }

        }

        [Fact]
        public void FunctionalityTest2()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                AssertComplexEmployee(str, ptr, employee, blittableContext);
            }
        }

        [Fact]
        public void EmptyArrayTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = "{\"Alias\":\"Jimmy\",\"Data\":[],\"Name\":\"Trolo\",\"SubData\":{\"SubArray\":[]}}";
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
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
            byte* encodeInput = (byte*)Marshal.AllocHGlobal(size);
            int compressedSize;
            byte* encodeOutput;
            var lz4 = new LZ4();

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

            byte* ptr;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
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
            }
        }

    }
}
