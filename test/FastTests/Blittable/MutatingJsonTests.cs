using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class MutatingJsonTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const string InitialJson = @"{""Name"":""Oren"",""Dogs"":[""Arava"",""Oscar"",""Sunny""],""State"":{""Sleep"":false}}";

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanModifyRespectingUnitOfWork()
        {
            var ctx = JsonOperationContext.ShortTermSingleUse();
            var ms = new MemoryStream(Encoding.UTF8.GetBytes("{\"item\": {\"age\": 2}}"));
            var obj = await ctx.ReadForMemoryAsync(ms, "fo");
            obj.TryGet("item", out BlittableJsonReaderObject b);

            b.Modifications = new DynamicJsonValue(b) { ["name"] = "fa" };

            BlittableJsonReaderObject blittableJsonReaderObject = ctx.ReadObject(obj, "a");
            Assert.Equal("fa", ((BlittableJsonReaderObject)blittableJsonReaderObject["item"])["name"]?.ToString());
        }
    }
}
