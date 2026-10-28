using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SystemTextJsonNewtonsoftCompatibilityTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    public class Animal
    {
        public string Name { get; set; }
    }

    public class Dog : Animal
    {
        public bool Barks { get; set; }
    }

    public interface IOwner
    {
        string Name { get; set; }
    }

    public class Person : IOwner
    {
        public string Name { get; set; }

        public int Age { get; set; }
    }

    public class Zoo
    {
        public Animal Star { get; set; }

        public Animal Plain { get; set; }

        public IOwner Owner { get; set; }

        public object Anything { get; set; }

        public object Text { get; set; }

        public List<Animal> Animals { get; set; }

        public Dictionary<string, Animal> AnimalsByName { get; set; }

        public IDictionary<string, string> Tags { get; set; }

        public List<object> Objects { get; set; }

        public IEnumerable<object> ObjectsAsEnumerable { get; set; }

        public object Strings { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesTheSameTypeNamesAsNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(CreateZoo(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(CreateZoo(), context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
            AssertTypeNamesComeFirst(actualJson);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsTypeNamesWrittenByNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = Newtonsoft.Serialization.DefaultConverter.ToBlittable(CreateZoo(), context))
            AssertZoo(SystemTextJson.Serialization.DefaultConverter.FromBlittable<Zoo>(json));
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void NewtonsoftReadsTypeNamesWrittenBySystemTextJson()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = SystemTextJson.Serialization.DefaultConverter.ToBlittable(CreateZoo(), context))
            AssertZoo(Newtonsoft.Serialization.DefaultConverter.FromBlittable<Zoo>(json));
    }

    public class Untyped
    {
        public object Data { get; set; }

        public ExpandoObject Expando { get; set; }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void UntypedValuesBehaveLikeNewtonsoftUnderDynamic(bool useSystemTextJson)
    {
        DocumentConventions conventions = useSystemTextJson ? SystemTextJson : Newtonsoft;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"Data\":{\"Name\":\"x\",\"Count\":5,\"Ratio\":1.5,\"Tags\":[\"a\",\"b\"],\"Nested\":{\"A\":null}},\"Expando\":{\"Name\":\"Grisha\",\"Age\":3}}", "untyped"))
        {
            var untyped = conventions.Serialization.DefaultConverter.FromBlittable<Untyped>(json);

            dynamic data = untyped.Data;
            Assert.True(data.Name == "x");
            Assert.True(data.Count == 5L);
            Assert.True(data.Ratio == 1.5m);
            Assert.True(data.Missing == null);
            Assert.True(data.Tags[0] == "a");
            Assert.Equal(2, (int)data.Tags.Count);
            Assert.True(data.Nested.A == null);
            Assert.Null(data.Nested.A.Value);
            Assert.Equal("x", (string)data.Name);
            Assert.Equal("x", (string)data.Name.Value);

            IDictionary<string, object> expando = untyped.Expando;
            Assert.Equal("Grisha", expando["Name"]);
            Assert.Equal(3L, expando["Age"]);
        }
    }

    public class VectorHolder
    {
        public RavenVector<ulong> Unsigned { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Vector)]
    public void UnsignedVectorsRoundTripExactly()
    {
        ulong[] values = [4552215358626332700UL, 18004558979925017000UL];
        var holder = new VectorHolder { Unsigned = new RavenVector<ulong>(values) };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            using (BlittableJsonReaderObject written = SystemTextJson.Serialization.DefaultConverter.ToBlittable(holder, context))
            using (BlittableJsonReaderObject parsed = context.Sync.ReadForMemory(written.ToString(), "parsed"))
                Assert.Equal(values, SystemTextJson.Serialization.DefaultConverter.FromBlittable<VectorHolder>(parsed).Unsigned.Embedding);

            using (BlittableJsonReaderObject written = Newtonsoft.Serialization.DefaultConverter.ToBlittable(holder, context))
                Assert.Equal(values, SystemTextJson.Serialization.DefaultConverter.FromBlittable<VectorHolder>(written).Unsigned.Embedding);
        }
    }

    private static DocumentConventions Newtonsoft { get; } = new() { Serialization = new NewtonsoftJsonSerializationConventions() };

    private static DocumentConventions SystemTextJson { get; } = new() { Serialization = new SystemTextJsonSerializationConventions() };

    private static Zoo CreateZoo()
    {
        return new Zoo
        {
            Star = new Dog { Name = "Rex", Barks = true },
            Plain = new Animal { Name = "Generic" },
            Owner = new Person { Name = "Oren", Age = 40 },
            Anything = new Dog { Name = "Max", Barks = false },
            Text = "hello",
            Animals = [new Dog { Name = "Fido", Barks = true }, new Animal { Name = "Cat" }],
            AnimalsByName = new Dictionary<string, Animal> { ["rex"] = new Dog { Name = "Rex", Barks = true }, ["cat"] = new Animal { Name = "Cat" } },
            Tags = new Dictionary<string, string> { ["color"] = "brown" },
            Objects = ["text", new Dog { Name = "Bolt", Barks = true }],
            ObjectsAsEnumerable = new List<object> { new Animal { Name = "Nemo" } },
            Strings = new List<string> { "a", "b" }
        };
    }

    private static void AssertZoo(Zoo zoo)
    {
        Assert.IsType<Dog>(zoo.Star);
        Assert.True(((Dog)zoo.Star).Barks);
        Assert.IsType<Animal>(zoo.Plain);
        Assert.Equal("Oren", Assert.IsType<Person>(zoo.Owner).Name);
        Assert.Equal("Max", Assert.IsType<Dog>(zoo.Anything).Name);
        Assert.IsType<Dog>(zoo.Animals[0]);
        Assert.IsType<Animal>(zoo.Animals[1]);
        Assert.IsType<Dog>(zoo.AnimalsByName["rex"]);
        Assert.IsType<Animal>(zoo.AnimalsByName["cat"]);
        Assert.Equal(["color"], zoo.Tags.Keys.ToArray());
        Assert.Equal("brown", zoo.Tags["color"]);
        Assert.Equal("Bolt", Assert.IsType<Dog>(zoo.Objects[1]).Name);
        Assert.Equal("Nemo", Assert.IsType<Animal>(Assert.Single(zoo.ObjectsAsEnumerable)).Name);
    }

    private static void AssertTypeNamesComeFirst(JToken token)
    {
        if (token is JObject obj)
        {
            List<JProperty> properties = obj.Properties().ToList();
            for (int i = 1; i < properties.Count; i++)
                Assert.NotEqual("$type", properties[i].Name);

            foreach (JProperty property in properties)
                AssertTypeNamesComeFirst(property.Value);
        }
        else if (token is JArray array)
        {
            foreach (JToken item in array)
                AssertTypeNamesComeFirst(item);
        }
    }
}
