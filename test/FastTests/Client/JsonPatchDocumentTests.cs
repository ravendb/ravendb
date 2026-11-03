using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using MicrosoftJsonPatchDocument = Microsoft.AspNetCore.JsonPatch.JsonPatchDocument;

namespace FastTests.Client;

public class JsonPatchDocumentTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly DateTime Timestamp = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    [RavenFact(RavenTestCategory.Patching)]
    public void OperationsMatchMicrosoftJsonPatchDocument()
    {
        var (raven, microsoft) = BuildBoth();

        Assert.Equal(microsoft.Operations.Count, raven.Operations.Count);
        for (int i = 0; i < raven.Operations.Count; i++)
        {
            Assert.Equal(microsoft.Operations[i].op, raven.Operations[i].Op);
            Assert.Equal(microsoft.Operations[i].path, raven.Operations[i].Path);
            Assert.Equal(microsoft.Operations[i].from, raven.Operations[i].From);
            Assert.Equal(microsoft.Operations[i].value, raven.Operations[i].Value);
        }
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData(false)]
    [InlineData(true)]
    public void WireFormatMatchesMicrosoftJsonPatchDocumentSerialization(bool useSystemTextJson)
    {
        var (raven, microsoft) = BuildBoth();
        ISerializationConventions newtonsoft = NewtonsoftTestConventions.Default.Serialization;
        ISerializationConventions serialization = new DocumentConventions
        {
            Serialization = useSystemTextJson ? new SystemTextJsonSerializationConventions() : new NewtonsoftJsonSerializationConventions()
        }.Serialization;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            IJsonSerializer serializer = newtonsoft.CreateSerializer(new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.None });
            List<BlittableJsonReaderObject> ravenOperations = raven.ToBlittable(serialization, context);

            for (int i = 0; i < microsoft.Operations.Count; i++)
            {
                BlittableJsonReaderObject expected = newtonsoft.DefaultConverter.ToBlittable(microsoft.Operations[i], context, serializer);

                Assert.True(JToken.DeepEquals(JObject.Parse(expected.ToString()), JObject.Parse(ravenOperations[i].ToString())),
                    $"Expected {expected} but got {ravenOperations[i]}");
            }
        }
    }

    [RavenFact(RavenTestCategory.Patching)]
    public void InvalidPathsAreRejected()
    {
        var patch = new JsonPatchDocument();

        Assert.Throws<ArgumentException>(() => patch.Add("/a//b", 1));
        Assert.Throws<ArgumentException>(() => patch.Move("/a//b", "/c"));
        Assert.Throws<ArgumentNullException>(() => patch.Remove(null));
        Assert.Throws<ArgumentNullException>(() => patch.Copy(null, "/c"));
    }

    private static (JsonPatchDocument Raven, MicrosoftJsonPatchDocument Microsoft) BuildBoth()
    {
        var raven = new JsonPatchDocument();
        var microsoft = new MicrosoftJsonPatchDocument();

        object nested = new { Visits = 32, Tags = new List<string> { "a", "b" } };
        int[] numbers = [1, 2];

        raven.Add("Name", "Hibernating Rhinos").Add("/Visits", 5).Add("/Missing", null).Add("/Nested", nested).Add("/When", Timestamp);
        microsoft.Add("Name", "Hibernating Rhinos").Add("/Visits", 5).Add("/Missing", null).Add("/Nested", nested).Add("/When", Timestamp);

        raven.Replace("/Numbers", numbers).Test("/Numbers", numbers).Remove("Old");
        microsoft.Replace("/Numbers", numbers).Test("/Numbers", numbers).Remove("Old");

        raven.Move("From", "/To").Copy("/To", "Copy");
        microsoft.Move("From", "/To").Copy("/To", "Copy");

        return (raven, microsoft);
    }
}
