using System;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SerializationConventionsJsonTextTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private class ToolResponse
    {
        public string City { get; set; }

        public double Temperature { get; set; }

        public List<string> Warnings { get; set; }

        public Dictionary<string, int> Counts { get; set; }
    }

    public static IEnumerable<object[]> Values =>
    [
        [new ToolResponse { City = "Hadera", Temperature = 21.5, Warnings = ["wind", "say \"hi\""], Counts = new Dictionary<string, int> { ["a"] = 1 } }],
        [new List<ToolResponse> { new() { City = "a" }, new() { City = "b", Temperature = 1 } }],
        [new[] { "one", "two" }],
        [new Dictionary<string, object> { ["Nested"] = new Dictionary<string, object> { ["Depth"] = 2 }, ["Flag"] = true }]
    ];

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(Values))]
    public void ValuesAreSerializedToTheSameJsonTextByBothSerializers(object value)
    {
        JToken newtonsoft = JToken.Parse(SerializeToJson(new NewtonsoftJsonSerializationConventions(), value));
        JToken systemTextJson = JToken.Parse(SerializeToJson(new SystemTextJsonSerializationConventions(), value));

        Assert.True(JToken.DeepEquals(newtonsoft, systemTextJson), $"Expected {newtonsoft} but got {systemTextJson}");
        Assert.Equal(value is ToolResponse or IDictionary<string, object> ? JTokenType.Object : JTokenType.Array, systemTextJson.Type);
    }

    private static string SerializeToJson(ISerializationConventions serialization, object value)
    {
        var conventions = new Raven.Client.Documents.Conventions.DocumentConventions { Serialization = serialization };
        MethodInfo method = typeof(DocumentStore).Assembly
            .GetType("Raven.Client.Json.Serialization.SerializationConventionsExtensions", throwOnError: true)
            .GetMethod("SerializeToJson", BindingFlags.Public | BindingFlags.Static);

        using (var context = JsonOperationContext.ShortTermSingleUse())
            return (string)method.Invoke(null, [conventions.Serialization, value, context]);
    }
}
