using System;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class ClientDtoJsonIgnoreTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private class UserDocument
    {
        public string Name { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        public string Secret { get; set; }
    }

    public enum Serializer
    {
        Newtonsoft,
        NewtonsoftWithCustomContractResolver,
        SystemTextJson
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(Serializer.Newtonsoft)]
    [InlineData(Serializer.NewtonsoftWithCustomContractResolver)]
    [InlineData(Serializer.SystemTextJson)]
    public void IgnoredClientDtoMembersAreNotSerialized(Serializer serializer)
    {
        var conventions = CreateConventions(serializer);
        var result = new TimeSeriesRawResult
        {
            Count = 1,
            Results = [new TimeSeriesEntry { Timestamp = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), Values = [1.5] }]
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(result, context))
        {
            Assert.True(json.TryGet(nameof(TimeSeriesRawResult.Results), out BlittableJsonReaderArray results));
            Assert.Equal(1, results.Length);
            Assert.False(json.TryGet(nameof(TimeSeriesRawResult.Stream), out object _));
            Assert.False(json.TryGet(nameof(TimeSeriesRawResult.StreamAsync), out object _));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void NewtonsoftSerializesUserMembersMarkedWithSystemTextJsonIgnore()
    {
        var conventions = CreateConventions(Serializer.Newtonsoft);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new UserDocument { Name = "a", Secret = "s" }, context))
        {
            Assert.True(json.TryGet(nameof(UserDocument.Secret), out string secret));
            Assert.Equal("s", secret);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void PlainSystemTextJsonSkipsTheUnsupportedTransformsOfAiConfigurations()
    {
        var options = new System.Text.Json.JsonSerializerOptions { IncludeFields = true };
        const string json = """{"Name":"ai","Transforms":[{"Name":"legacy","Collections":["Orders"]}]}""";

        var genAi = System.Text.Json.JsonSerializer.Deserialize<GenAiConfiguration>(json, options);
        Assert.Equal("ai", genAi.Name);

        var embeddings = System.Text.Json.JsonSerializer.Deserialize<EmbeddingsGenerationConfiguration>(json, options);
        Assert.Equal("ai", embeddings.Name);
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(Serializer.Newtonsoft)]
    [InlineData(Serializer.NewtonsoftWithCustomContractResolver)]
    [InlineData(Serializer.SystemTextJson)]
    public void RavenSerializersSkipTheUnsupportedTransformsOfAiConfigurations(Serializer serializer)
    {
        var conventions = CreateConventions(serializer);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.ReadObject(new DynamicJsonValue
        {
            ["Name"] = "ai",
            ["Transforms"] = new DynamicJsonArray { new DynamicJsonValue { ["Name"] = "legacy" } }
        }, "ai"))
        {
            Assert.Equal("ai", conventions.Serialization.DefaultConverter.FromBlittable<GenAiConfiguration>(json).Name);
            Assert.Equal("ai", conventions.Serialization.DefaultConverter.FromBlittable<EmbeddingsGenerationConfiguration>(json).Name);
        }
    }

    private static DocumentConventions CreateConventions(Serializer serializer)
    {
        return new DocumentConventions
        {
            Serialization = serializer switch
            {
                Serializer.SystemTextJson => new SystemTextJsonSerializationConventions(),
                Serializer.NewtonsoftWithCustomContractResolver => new NewtonsoftJsonSerializationConventions { JsonContractResolver = new DefaultContractResolver() },
                _ => new NewtonsoftJsonSerializationConventions()
            }
        };
    }
}
