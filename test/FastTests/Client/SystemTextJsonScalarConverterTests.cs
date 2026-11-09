using System;
using System.Collections.Generic;
using System.Numerics;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SystemTextJsonScalarConverterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private class HoldsBigInteger
    {
        public BigInteger Value { get; set; }
    }

    private class HoldsChar
    {
        public char Value { get; set; }

        public List<char> Many { get; set; }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("42")]
    [InlineData("-42")]
    [InlineData("18446744073709551615")]
    [InlineData("12345678901234567890123")]
    [InlineData("-12345678901234567890123")]
    [InlineData("\"12345678901234567890123\"")]
    public void BigIntegersAreReadLikeNewtonsoft(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + "}", "big-integer"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);

            Assert.Equal(BigInteger.Parse(json.Trim('"')), expected.Value);
            Assert.Equal(expected.Value, actual.Value);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void BigIntegersAreReadFromFloatingPointNumbers()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":1.5e3}", "big-integer"))
        {
            Assert.Equal(new BigInteger(1500), SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable).Value);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("\"a\"", 'a')]
    [InlineData("\"\\u00e9\"", '\u00e9')]
    [InlineData("\"\u017c\"", '\u017c')]
    [InlineData("\"\\\"\"", '"')]
    [InlineData("97", 'a')]
    public void CharsAreReadLikeNewtonsoft(string json, char expected)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + ",\"Many\":[" + json + "," + json + "]}", "char"))
        {
            var newtonsoft = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable);
            var systemTextJson = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable);

            Assert.Equal(expected, newtonsoft.Value);
            Assert.Equal(expected, systemTextJson.Value);
            Assert.Equal(new[] { expected, expected }, systemTextJson.Many);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("\"ab\"")]
    [InlineData("\"\"")]
    [InlineData("\"\\uD83D\\uDE00\"")]
    [InlineData("\"this is far too long for one char\"")]
    public void CharsRejectStringsOfOtherLengths(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + "}", "char"))
        {
            Assert.ThrowsAny<Exception>(() => SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void CharsAreWrittenLikeNewtonsoft()
    {
        var value = new HoldsChar { Value = '\u017c', Many = ['a', '"', '\\', '\u00e9', '\0'] };

        var read = AssertWritesLikeNewtonsoft(value);
        Assert.Equal(value.Value, read.Value);
        Assert.Equal(value.Many, read.Many);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void CharMetadataValuesAreWrittenLikeNewtonsoft()
    {
        var metadata = new MetadataAsDictionary
        {
            ["Ascii"] = 'a',
            ["Wide"] = '\u017c'
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new HoldsChar(), metadata, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new HoldsChar(), metadata, context))
        {
            var expectedMetadata = (BlittableJsonReaderObject)expected[Raven.Client.Constants.Documents.Metadata.Key];
            var actualMetadata = (BlittableJsonReaderObject)actual[Raven.Client.Constants.Documents.Metadata.Key];

            Assert.Equal("a", actualMetadata["Ascii"].ToString());
            Assert.Equal("\u017c", actualMetadata["Wide"].ToString());
            Assert.Equal(expectedMetadata["Ascii"].ToString(), actualMetadata["Ascii"].ToString());
            Assert.Equal(expectedMetadata["Wide"].ToString(), actualMetadata["Wide"].ToString());
        }
    }

    private static T AssertWritesLikeNewtonsoft<T>(T value)
    {
        return AssertWritesLikeNewtonsoft(value, Newtonsoft, SystemTextJson);
    }

    private static T AssertWritesLikeNewtonsoft<T>(T value, DocumentConventions newtonsoft, DocumentConventions systemTextJson)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            return systemTextJson.Serialization.DefaultConverter.FromBlittable<T>(actual);
        }
    }

    private static DocumentConventions Newtonsoft { get; } = new() { Serialization = new NewtonsoftJsonSerializationConventions() };

    private static DocumentConventions SystemTextJson { get; } = new() { Serialization = new SystemTextJsonSerializationConventions() };

    private static DocumentConventions NewtonsoftIntegers { get; } = new() { SaveEnumsAsIntegers = true, Serialization = new NewtonsoftJsonSerializationConventions() };

    private static DocumentConventions SystemTextJsonIntegers { get; } = new() { SaveEnumsAsIntegers = true, Serialization = new SystemTextJsonSerializationConventions() };
}
