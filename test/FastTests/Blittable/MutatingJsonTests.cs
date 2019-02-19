// -----------------------------------------------------------------------
//  <copyright file="MutatingJsonTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class MutatingJsonTests : NoDisposalNeeded
    {
        private const string InitialJson = @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false}}";

        [Fact]
        public void CanAddProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":34}");
        }

        [Fact]
        public void CanAddNegativeIntegerProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = -34
                };
            }, @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false},""Age"":-34}");
        }

        [Fact]
        public void CanCompressFields()
        {
            AssertEqualAfterRoundTrip(source =>
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
        public void WillPreserveEscapes()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue
                {
                    ["Age"] = 34
                };
            }, @"{""Name"":""Oren\r\n"",""Age"":34}",
                @"{""Name"":""Oren\r\n""}");
        }


        [Fact]
        public void CanModifyArrayProperty()
        {
            AssertEqualAfterRoundTrip(source =>
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
        public void CanModifyNestedObjectProperty()
        {
            AssertEqualAfterRoundTrip(source =>
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
        public void CanRemoveAndAddProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {
                    ["Pie"] = 3.147
                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false},""Pie"":3.147}");
        }

        [Fact]
        public void CanAddAndRemoveProperty()
        {
            AssertEqualAfterRoundTrip(source =>
            {
                source.Modifications = new DynamicJsonValue(source)
                {

                };
                source.Modifications.Remove("Dogs");
            }, @"{""Name"":""Oren"",""State"":{""Sleep"":false}}");
        }

        private static void AssertEqualAfterRoundTrip(Action<BlittableJsonReaderObject> mutate, string expected, string json = null)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(json ?? InitialJson);
                streamWriter.Flush();
                stream.Position = 0;
                using (var writer = ctx.Read(stream, "foo"))
                {
                    mutate(writer);
                    using (var document = ctx.ReadObject(writer, "foo"))
                    {
                        var ms = new MemoryStream();
                        ctx.Write(ms, document);
                        var actual = Encoding.UTF8.GetString(ms.ToArray());
                        Assert.Equal(expected, actual);
                    }
                }
            }
        }
    }
}
