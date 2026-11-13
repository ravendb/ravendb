using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SystemTextJsonSessionWriteTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Reading
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public double Value { get; set; }

        public decimal Precise { get; set; }

        public List<string> Tags { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task NewEntitiesAreWrittenLikeNewtonsoft()
    {
        await AssertSessionWritesLikeNewtonsoft(async session =>
        {
            await session.StoreAsync(CreateReading(), "readings/1");
            await session.SaveChangesAsync();
        });
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task NewEntitiesWithMetadataInstancesAreWrittenLikeNewtonsoft()
    {
        await AssertSessionWritesLikeNewtonsoft(async session =>
        {
            Reading reading = CreateReading();
            await session.StoreAsync(reading, "readings/1");

            IMetadataDictionary metadata = session.Advanced.GetMetadataFor(reading);
            metadata["Source"] = "sensor";
            metadata["Nested"] = new Dictionary<string, object> { ["Depth"] = 2, ["Names"] = new[] { "a", "b" } };
            metadata["Readings"] = new object[] { 1, 2.5, "three", null };
            metadata["When"] = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

            await session.SaveChangesAsync();
        });
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadedEntitiesKeepTheirMetadataLikeNewtonsoft()
    {
        await AssertSessionWritesLikeNewtonsoft(async session =>
        {
            Reading reading = CreateReading();
            await session.StoreAsync(reading, "readings/1");
            IMetadataDictionary metadata = session.Advanced.GetMetadataFor(reading);
            metadata["Nested"] = new Dictionary<string, object> { ["Depth"] = 2, ["Names"] = new[] { "a", "b" } };
            metadata["Ratio"] = 0.25;
            await session.SaveChangesAsync();
        }, async session =>
        {
            var reading = await session.LoadAsync<Reading>("readings/1");
            reading.Name = "changed";
            await session.SaveChangesAsync();
        });
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task MetadataModificationsOfLoadedEntitiesAreWrittenLikeNewtonsoft()
    {
        await AssertSessionWritesLikeNewtonsoft(async session =>
        {
            await session.StoreAsync(CreateReading(), "readings/1");
            await session.SaveChangesAsync();
        }, async session =>
        {
            var reading = await session.LoadAsync<Reading>("readings/1");
            IMetadataDictionary metadata = session.Advanced.GetMetadataFor(reading);
            metadata["Extra"] = "x";
            metadata["Nested"] = new Dictionary<string, object> { ["Depth"] = 3 };
            await session.SaveChangesAsync();
        });
    }

    private static Reading CreateReading()
    {
        return new Reading
        {
            Name = "r",
            Value = double.NaN,
            Precise = 1234567890.123456789m,
            Tags = ["x", "y"]
        };
    }

    private async Task AssertSessionWritesLikeNewtonsoft(params Func<IAsyncDocumentSession, Task>[] steps)
    {
        JObject expected = await Run(new NewtonsoftJsonSerializationConventions(), steps);
        JObject actual = await Run(new SystemTextJsonSerializationConventions(), steps);

        Assert.True(JToken.DeepEquals(expected, actual), $"Expected {expected} but got {actual}");
    }

    private async Task<JObject> Run(ISerializationConventions serialization, Func<IAsyncDocumentSession, Task>[] steps)
    {
        using var store = GetDocumentStore(new Options { ModifyDocumentStore = s => s.Conventions.Serialization = serialization });

        foreach (Func<IAsyncDocumentSession, Task> step in steps)
        {
            using (IAsyncDocumentSession session = store.OpenAsyncSession())
                await step(session);
        }

        using (IAsyncDocumentSession session = store.OpenAsyncSession())
        {
            var document = await session.LoadAsync<BlittableJsonReaderObject>("readings/1");
            JObject json = JObject.Parse(document.ToString());
            var metadata = (JObject)json["@metadata"];
            metadata.Remove("@change-vector");
            metadata.Remove("@last-modified");
            Assert.Equal("Readings", (string)metadata["@collection"]);
            Assert.Equal("readings/1", (string)metadata["@id"]);
            return json;
        }
    }
}
