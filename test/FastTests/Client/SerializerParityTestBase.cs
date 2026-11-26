using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client;

public abstract class SerializerParityTestBase(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    protected static DocumentConventions Newtonsoft { get; } = new() { Serialization = new NewtonsoftJsonSerializationConventions() };

    protected static DocumentConventions SystemTextJson { get; } = new() { Serialization = new SystemTextJsonSerializationConventions() };

    protected static T AssertWritesLikeNewtonsoft<T>(T value)
    {
        return AssertWritesLikeNewtonsoft(value, Newtonsoft, SystemTextJson);
    }

    protected static T AssertWritesLikeNewtonsoft<T>(T value, DocumentConventions newtonsoft, DocumentConventions systemTextJson)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            return systemTextJson.Serialization.DefaultConverter.FromBlittable<T>(expected);
        }
    }
}
