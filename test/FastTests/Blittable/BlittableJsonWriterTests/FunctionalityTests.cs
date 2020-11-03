using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Compression;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class FunctionalityTests : BlittableJsonTestBase
    {
        public FunctionalityTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Generating_DictionaryDeserializationRoutine_should_work()
        {
            Func<BlittableJsonReaderObject, Dictionary<string, long>> deserializationFunc = JsonDeserializationBase.GenerateJsonDeserializationRoutine<Dictionary<string, long>>();
            Assert.NotNull(deserializationFunc);
        }

        [Fact]
        public async Task FunctionalityTest()
        {
            var str = GenerateSimpleEntityForFunctionalityTest();
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = await blittableContext.ReadForDiskAsync(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicRavenJObject = JsonConvert.DeserializeObject<ExpandoObject>(str, new ExpandoObjectConverter());
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
                await blittableContext.WriteAsync(ms, employee);
                Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
            }
        }

        [Fact]
        public void FunctionalityTest2()
        {
            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                AssertComplexEmployee(str, employee, blittableContext);
            }
        }

        [Fact]
        public void EmptyArrayTest()
        {
            var str = "{\"Alias\":\"Jimmy\",\"Data\":[],\"Name\":\"Trolo\",\"SubData\":{\"SubArray\":[]}}";
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var employee = blittableContext.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
            {
                dynamic dynamicObject = new DynamicBlittableJson(employee);
                Assert.Equal(dynamicObject.Alias, "Jimmy");
                Assert.Equal(dynamicObject.Data.Length, 0);
                Assert.Equal(dynamicObject.SubData.SubArray.Length, 0);
                Assert.Equal(dynamicObject.Name, "Trolo");
                Assert.Throws<ArgumentOutOfRangeException>(() => dynamicObject.Data[0]);
                Assert.Throws<ArgumentOutOfRangeException>(() => dynamicObject.SubData.SubArray[0]);
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
            byte* encodeInput = NativeMemory.AllocateMemory(size);
            int compressedSize;
            byte* encodeOutput;

            var originStr = string.Join("", Enumerable.Repeat(1, size).Select(x => "sample"));
            var bytes = Encoding.UTF8.GetBytes(originStr);
            var maximumOutputLength = LZ4.MaximumOutputLength(bytes.Length);
            fixed (byte* pb = bytes)
            {
                encodeOutput = NativeMemory.AllocateMemory((int)maximumOutputLength);
                compressedSize = LZ4.Encode64(pb, encodeOutput, bytes.Length, (int)maximumOutputLength);
            }

            Array.Clear(bytes, 0, bytes.Length);
            fixed (byte* pb = bytes)
            {
                LZ4.Decode64(encodeOutput, compressedSize, pb, bytes.Length, true);
            }
            var actual = Encoding.UTF8.GetString(bytes);
            Assert.Equal(originStr, actual);

            NativeMemory.Free(encodeInput, size);
            NativeMemory.Free(encodeOutput, maximumOutputLength);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task LongStringsTest(int repeatSize)
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

            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            using (var doc = await blittableContext.ReadForDiskAsync(new MemoryStream(Encoding.UTF8.GetBytes(str)), "doc1"))
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
                await blittableContext.WriteAsync(ms, doc);
                Assert.Equal(str, Encoding.UTF8.GetString(ms.ToArray()));
            }
        }

        [Fact]
        public async Task BasicCopyToStream()
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

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var r = await ctx.ReadForDiskAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), "doc1"))
            {
                var ms = new MemoryStream();
                await ctx.WriteAsync(ms, r);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), json);
            }
        }

        [Fact]
        public async Task UsingBooleans()
        {
            var json = JsonConvert.SerializeObject(new
            {
                Name = "Oren",
                Dogs = true
            });

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var r = await ctx.ReadForDiskAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), "doc1"))
            {
                var ms = new MemoryStream();
                await ctx.WriteAsync(ms, r);

                Assert.Equal(Encoding.UTF8.GetString(ms.ToArray()), json);
            }
        }

        [Fact]
        public void UsingChars()
        {
            var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<CharClass>();

            var djv = new DynamicJsonValue
            {
                [nameof(CharClass.CharWithValue)] = "a"
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(djv, "json");
                var result = deserialize.Invoke(json);

                Assert.Equal('a', result.CharWithValue);
                Assert.Equal(default(char), result.CharNoValue);
            }
        }

        private class CharClass
        {
            public char CharWithValue { get; set; }
            public char CharNoValue { get; set; }
        }
    }
}
