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
