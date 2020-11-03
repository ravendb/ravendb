// -----------------------------------------------------------------------
//  <copyright file="MutatingJsonTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class MutatingJsonTests : NoDisposalNeeded
    {
        public MutatingJsonTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string InitialJson = @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false}}";

        [Fact]
        public Task CanAddProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":34}");
        }

        [Fact]
        public Task CanAddNegativeIntegerProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = -34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":-34}");
        }

        [Fact]
        public Task CanCompressFields()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            },
                @"{""Name"":""there goes the man in the moon"",""Age"":34}",
                @"{""Name"":""there goes the man in the moon""}");
        }

        [Fact]
        public Task WillPreserveEscapes()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren\r\n"",""Age"":34}",
                @"{""Name"":""Oren\r\n""}");
        }

        [Fact]
        public Task CanModifyArrayProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                object result;
                source.TryGetMember("Dogs", out result);
                var array = (BlittableJsonReaderArray)result;
                array.Modifications = new DynamicJsonArray
                {
                    "Phoebe"
                };
                array.Modifications.RemoveAt(2);
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Phoebe""],""State"":{""Sleep"":false}}");
        }

        [Fact]
        public Task CanModifyNestedObjectProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                object result;
                source.TryGetMember("State", out result);
                var array = (BlittableJsonReaderObject)result;
                array.Modifications = new DynamicJsonValue
                {
                    ["Sleep"] = true
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":true}}");
        }

        [Fact]
        public Task CanRemoveAndAddProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {
                    ["Pie"] = 3.147
                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false},""Pie"":3.147}");
        }

        [Fact]
        public Task CanAddAndRemoveProperty()
        {
            return AssertEqualAfterRoundTripAsync(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {
                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false}}");
        }

        private static async Task AssertEqualAfterRoundTripAsync(Action<BlittableJsonReaderObject> mutate, string expected, string json = null)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                await streamWriter.WriteAsync(json ?? InitialJson);
                await streamWriter.FlushAsync();
                stream.Position = 0;
                using (var writer = await ctx.ReadForDiskAsync(stream, "foo"))
                {
                    mutate(writer);
                    using (var document = ctx.ReadObject(writer, "foo"))
                    {
                        var ms = new MemoryStream();
                        await ctx.WriteAsync(ms, document);
                        var actual = Encoding.UTF8.GetString(ms.ToArray());
                        Assert.Equal(expected, actual);
                    }
                }
            }
        }
    }
}
