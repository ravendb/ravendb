// -----------------------------------------------------------------------
//  <copyright file="MutatingJsonTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Linq;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests
{
    public class MutatingJsonTests
    {
        private const string InitialJson = @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false}}";

        [Fact]
        public async Task CanAddProperty()
        {
            await AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":34}");
        }

        [Fact]
        public async Task CanCompressFields()
        {
            await AssertEqualAfterRoundTrip(source =>
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
        public async Task WillPreserveEscapes()
        {
            await AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren\r\n"",""Age"":34}",
                @"{""Name"":""Oren\r\n""}");
        }


        [Fact]
        public async Task CanModifyArrayProperty()
        {
            await AssertEqualAfterRoundTrip(source =>
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
        public async Task CanModifyNestedObjectProperty()
        {
            await AssertEqualAfterRoundTrip(source =>
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
        public async Task CanRemoveAndAddProperty()
        {
            await AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {
                    ["Pie"] = 3.147
                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false},""Pie"":3.147}");
        }

        [Fact]
        public async Task CanAddAndRemoveProperty()
        {
            await AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {

                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false}}");
        }

        private static async Task AssertEqualAfterRoundTrip(Action<BlittableJsonReaderObject> mutate, string expected, string json = null)
        {
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new RavenOperationContext(pool))
            {
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(json ?? InitialJson);
                streamWriter.Flush();
                stream.Position = 0;
                using (var writer = await ctx.Read(stream, "foo"))
                {
                    mutate(writer);
                    using (var document = await ctx.ReadObject(writer, "foo"))
                    {
                        var ms = new MemoryStream();
                        document.WriteTo(ms, originalPropertyOrder: true);
                        var actual = Encoding.UTF8.GetString(ms.ToArray());
                        Assert.Equal(expected, actual);
                    }
                }
            }
        }
    }
}