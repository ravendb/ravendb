using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Raven.Client.ServerWide;
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

    private interface IHasSecret
    {
        string Secret { get; set; }
    }

    private class EncapsulatedBase
    {
        private string BasePrivate { get; set; }

        protected string BaseProtected { get; set; }

        public void SetBase(string basePrivate, string baseProtected)
        {
            BasePrivate = basePrivate;
            BaseProtected = baseProtected;
        }

        public string GetBasePrivate() => BasePrivate;

        public string GetBaseProtected() => BaseProtected;
    }

    private class Encapsulated : EncapsulatedBase, IHasSecret
    {
        private readonly List<string> _lines = [];

        public string Status { get; private set; }

        public string WriteOnly { private get; set; }

        internal string Note { get; set; }

        private string Hidden { get; set; }

        string IHasSecret.Secret { get; set; }

        public IReadOnlyList<string> Lines => _lines;

        public List<string> Tags { get; } = [];

        public HashSet<string> Labels { get; } = [];

        public Dictionary<string, int> Counts { get; } = new();

        public Person Keeper { get; } = new();

        public string GetWriteOnly() => WriteOnly;

        public string GetHidden() => Hidden;

        public static Encapsulated Create()
        {
            var encapsulated = new Encapsulated { Status = "Shipped", WriteOnly = "write-only", Note = "note", Hidden = "hidden" };
            ((IHasSecret)encapsulated).Secret = "secret";
            encapsulated.SetBase("base-private", "base-protected");
            encapsulated._lines.Add("line");
            encapsulated.Tags.Add("tag");
            encapsulated.Labels.Add("label");
            encapsulated.Counts["count"] = 3;
            encapsulated.Keeper.Name = "Oren";
            encapsulated.Keeper.Age = 40;
            return encapsulated;
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesNonPublicMembersLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(Encapsulated.Create(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(Encapsulated.Create(), context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void SystemTextJsonReadsNonPublicMembers(bool writtenByNewtonsoft)
    {
        DocumentConventions writer = writtenByNewtonsoft ? Newtonsoft : SystemTextJson;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = writer.Serialization.DefaultConverter.ToBlittable(Encapsulated.Create(), context))
        {
            var encapsulated = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Encapsulated>(json);

            Assert.Equal("Shipped", encapsulated.Status);
            Assert.Equal("write-only", encapsulated.GetWriteOnly());
            Assert.Equal("note", encapsulated.Note);
            Assert.Equal("hidden", encapsulated.GetHidden());
            Assert.Equal("secret", ((IHasSecret)encapsulated).Secret);
            Assert.Equal("base-private", encapsulated.GetBasePrivate());
            Assert.Equal("base-protected", encapsulated.GetBaseProtected());
            Assert.Equal(["line"], encapsulated.Lines);
            Assert.Equal(["tag"], encapsulated.Tags);
            Assert.Equal(["label"], encapsulated.Labels);
            Assert.Equal(3, encapsulated.Counts["count"]);
            Assert.Equal("Oren", encapsulated.Keeper.Name);
            Assert.Equal(40, encapsulated.Keeper.Age);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesAndReadsForcedNonPublicFields()
    {
        FieldInfo supportedFeatures = typeof(DatabaseRecord).GetField("SupportedFeatures", BindingFlags.Instance | BindingFlags.NonPublic);
        var record = new DatabaseRecord("db1");
        supportedFeatures.SetValue(record, new List<string> { "feature" });

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(record, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(record, context))
        {
            Assert.Equal(expected["SupportedFeatures"].ToString(), actual["SupportedFeatures"].ToString());

            var read = SystemTextJson.Serialization.DefaultConverter.FromBlittable<DatabaseRecord>(actual);
            Assert.Equal(["feature"], (IReadOnlyList<string>)supportedFeatures.GetValue(read));
        }
    }

    [DataContract]
    private class ContractRenames
    {
        [DataMember(Name = "contract_name")]
        public string Name { get; set; }

        [DataMember]
        public int Count { get; set; }
    }

    private class Renames
    {
        [JsonProperty("json_property")]
        public string Renamed { get; set; }

        [DataMember(Name = "outside_contract")]
        public string OutsideContract { get; set; }

        [JsonProperty("private_renamed")]
        private string PrivateRenamed { get; set; }

        public ContractRenames Contract { get; set; }

        public string GetPrivateRenamed() => PrivateRenamed;

        public static Renames Create() => new()
        {
            Renamed = "renamed",
            OutsideContract = "outside",
            PrivateRenamed = "private",
            Contract = new ContractRenames { Name = "contract", Count = 2 }
        };
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesNewtonsoftRenamesLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(Renames.Create(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(Renames.Create(), context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
            Assert.Equal("renamed", (string)actualJson["json_property"]);
            Assert.Equal("outside", (string)actualJson["OutsideContract"]);
            Assert.Equal("private", (string)actualJson["private_renamed"]);
            Assert.Equal("contract", (string)actualJson["Contract"]["contract_name"]);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void SystemTextJsonReadsNewtonsoftRenames(bool writtenByNewtonsoft)
    {
        DocumentConventions writer = writtenByNewtonsoft ? Newtonsoft : SystemTextJson;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = writer.Serialization.DefaultConverter.ToBlittable(Renames.Create(), context))
        {
            var renames = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Renames>(json);

            Assert.Equal("renamed", renames.Renamed);
            Assert.Equal("outside", renames.OutsideContract);
            Assert.Equal("private", renames.GetPrivateRenamed());
            Assert.Equal("contract", renames.Contract.Name);
            Assert.Equal(2, renames.Contract.Count);
        }
    }

    private class UpperCaseConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType) => objectType == typeof(string);

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) => writer.WriteValue(((string)value)?.ToUpperInvariant());

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) => reader.Value;
    }

    private class MemberWithNewtonsoftConverter
    {
        [JsonConverter(typeof(UpperCaseConverter))]
        public string Name { get; set; }
    }

    [JsonConverter(typeof(UpperCaseConverter))]
    private class TypeWithNewtonsoftConverter
    {
        public string Name { get; set; }
    }

    private class HoldsTypeWithNewtonsoftConverter
    {
        public TypeWithNewtonsoftConverter Value { get; set; }
    }

    private class MemberWithNewtonsoftExtensionData
    {
        public string Name { get; set; }

        [JsonExtensionData]
        public IDictionary<string, JToken> Extra { get; set; }
    }

    private class NewtonsoftConstructorAmongMany
    {
        public NewtonsoftConstructorAmongMany()
        {
        }

        [JsonConstructor]
        public NewtonsoftConstructorAmongMany(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    private class SingleNewtonsoftConstructor
    {
        [JsonConstructor]
        public SingleNewtonsoftConstructor(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    private readonly struct NewtonsoftStructConstructor
    {
        [JsonConstructor]
        public NewtonsoftStructConstructor(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    private class BothConstructorAttributes
    {
        public BothConstructorAttributes()
        {
        }

        [JsonConstructor]
        [System.Text.Json.Serialization.JsonConstructor]
        public BothConstructorAttributes(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    [JsonConverter(typeof(StringEnumConverter))]
    private enum Shade
    {
        Light,
        Dark
    }

    private class HoldsShade
    {
        public Shade Shade { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsUntranslatableNewtonsoftAttributes()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertRejected(new MemberWithNewtonsoftConverter { Name = "x" }, "Newtonsoft.Json.JsonConverterAttribute");
            AssertRejected(new HoldsTypeWithNewtonsoftConverter { Value = new TypeWithNewtonsoftConverter { Name = "x" } }, "Newtonsoft.Json.JsonConverterAttribute");
            AssertRejected(new MemberWithNewtonsoftExtensionData { Name = "x" }, "Newtonsoft.Json.JsonExtensionDataAttribute");
            AssertRejected(new NewtonsoftConstructorAmongMany("x"), "Newtonsoft.Json.JsonConstructorAttribute");
            AssertRejected(new NewtonsoftStructConstructor("x"), "Newtonsoft.Json.JsonConstructorAttribute");

            void AssertRejected(object entity, string attributeName)
            {
                var exception = Assert.Throws<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context));
                Assert.Contains(attributeName, exception.Message);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonAcceptsNewtonsoftAttributesWithTheSameOutcome()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            using (BlittableJsonReaderObject json = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new SingleNewtonsoftConstructor("single"), context))
                Assert.Equal("single", SystemTextJson.Serialization.DefaultConverter.FromBlittable<SingleNewtonsoftConstructor>(json).Name);

            using (BlittableJsonReaderObject json = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new BothConstructorAttributes("both"), context))
                Assert.Equal("both", SystemTextJson.Serialization.DefaultConverter.FromBlittable<BothConstructorAttributes>(json).Name);

            using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new HoldsShade { Shade = Shade.Dark }, context))
            using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new HoldsShade { Shade = Shade.Dark }, context))
            {
                Assert.Equal(expected.ToString(), actual.ToString());
                Assert.Equal(Shade.Dark, SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsShade>(expected).Shade);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsNewtonsoftStringEnumConverterWhenEnumsAreSavedAsIntegers()
    {
        var conventions = new DocumentConventions { SaveEnumsAsIntegers = true, Serialization = new SystemTextJsonSerializationConventions() };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            var exception = Assert.Throws<NotSupportedException>(() => conventions.Serialization.DefaultConverter.ToBlittable(new HoldsShade { Shade = Shade.Dark }, context));
            Assert.Contains("Newtonsoft.Json.JsonConverterAttribute", exception.Message);
        }
    }

    private enum Stage
    {
        [EnumMember(Value = "not-started")]
        NotStarted,

        [EnumMember(Value = "in-progress")]
        InProgress,

        Done
    }

    [Flags]
    private enum Access
    {
        None = 0,

        [EnumMember(Value = "read")]
        Read = 1,

        [EnumMember(Value = "write")]
        Write = 2
    }

    private class HoldsStage
    {
        public Stage Stage { get; set; }

        public Stage? Optional { get; set; }

        public List<Stage> History { get; set; }

        public Access Access { get; set; }

        public static HoldsStage Create() => new()
        {
            Stage = Stage.InProgress,
            Optional = Stage.NotStarted,
            History = [Stage.NotStarted, Stage.Done],
            Access = Access.Read | Access.Write
        };
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesEnumMemberValuesLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(HoldsStage.Create(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(HoldsStage.Create(), context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
            Assert.Equal("in-progress", (string)actualJson["Stage"]);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void SystemTextJsonReadsEnumMemberValues(bool writtenByNewtonsoft)
    {
        DocumentConventions writer = writtenByNewtonsoft ? Newtonsoft : SystemTextJson;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = writer.Serialization.DefaultConverter.ToBlittable(HoldsStage.Create(), context))
        {
            var holder = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsStage>(json);

            Assert.Equal(Stage.InProgress, holder.Stage);
            Assert.Equal(Stage.NotStarted, holder.Optional);
            Assert.Equal([Stage.NotStarted, Stage.Done], holder.History);
            Assert.Equal(Access.Read | Access.Write, holder.Access);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsEnumNamesWhenValuesAreDefined()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Stage\":\"InProgress\",\"Optional\":\"notstarted\",\"History\":[\"Done\"],\"Access\":\"Read, Write\"}", "names"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsStage>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsStage>(json);

            Assert.Equal(expected.Stage, actual.Stage);
            Assert.Equal(expected.Optional, actual.Optional);
            Assert.Equal(expected.History, actual.History);
            Assert.Equal(expected.Access, actual.Access);
        }
    }

    private class HoldsNewtonsoftLinq
    {
        public JObject Data { get; set; }
    }

    private class HoldsSystemTextJsonNodes
    {
        public System.Text.Json.Nodes.JsonNode Node { get; set; }

        public System.Text.Json.JsonElement Element { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsNewtonsoftLinqTypes()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertRejected(new HoldsNewtonsoftLinq { Data = JObject.Parse("{\"Name\":\"x\"}") });
            AssertRejected(new Untyped { Data = JObject.Parse("{\"Name\":\"x\"}") });
            AssertRejected(JObject.Parse("{\"Name\":\"x\"}"));

            void AssertRejected(object entity)
            {
                var exception = Assert.Throws<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context));
                Assert.Contains("Newtonsoft.Json.Linq.JObject", exception.Message);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRoundTripsJsonNodeAndJsonElement()
    {
        var holder = new HoldsSystemTextJsonNodes
        {
            Node = System.Text.Json.Nodes.JsonNode.Parse("{\"Name\":\"x\",\"Tags\":[\"a\"]}"),
            Element = System.Text.Json.JsonDocument.Parse("{\"Count\":2}").RootElement
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = SystemTextJson.Serialization.DefaultConverter.ToBlittable(holder, context))
        {
            var read = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsSystemTextJsonNodes>(json);

            Assert.Equal("x", (string)read.Node["Name"]);
            Assert.Equal("a", (string)read.Node["Tags"][0]);
            Assert.Equal(2, read.Element.GetProperty("Count").GetInt32());
        }
    }

    private class Marker
    {
    }

    private class HoldsObjectArray
    {
        public object[] Mixed { get; set; }

        public object[] Covariant { get; set; }

        public object Boxed { get; set; }

        public static HoldsObjectArray Create() => new()
        {
            Mixed = [new Marker(), new Dog { Name = "Rex", Barks = true }],
            Covariant = new[] { new Marker() },
            Boxed = new[] { new Marker() }
        };
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesTypeNamesForObjectArrayItemsLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(HoldsObjectArray.Create(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(HoldsObjectArray.Create(), context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsCovariantArrayItemsWrittenByNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = Newtonsoft.Serialization.DefaultConverter.ToBlittable(HoldsObjectArray.Create(), context))
        {
            var holder = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsObjectArray>(json);

            Assert.IsType<Marker>(Assert.Single(holder.Covariant));
            Assert.IsType<Marker>(holder.Mixed[0]);
            Assert.IsType<Dog>(holder.Mixed[1]);
        }
    }

    private class HiddenBase
    {
        public double[] Max;

        public string Label { get; set; }
    }

    private class HidesWithIgnoredMembers : HiddenBase
    {
        [JsonIgnore]
        public new int Max => base.Max?.Length ?? 0;

        [JsonIgnore]
        public new string Label => base.Label?.ToUpperInvariant();
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSerializesBaseMembersHiddenByIgnoredMembersLikeNewtonsoft()
    {
        var value = new HidesWithIgnoredMembers();
        ((HiddenBase)value).Max = [1.5, 2.5];
        ((HiddenBase)value).Label = "label";

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            var read = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HidesWithIgnoredMembers>(expected);
            Assert.Equal([1.5, 2.5], ((HiddenBase)read).Max);
            Assert.Equal("label", ((HiddenBase)read).Label);
        }
    }

    private class StockPrice
    {
        [TimeSeriesValue(0)]
        public double Open;

        [TimeSeriesValue(1)]
        public double Close;
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
    public void SystemTextJsonReadsTypedTimeSeriesAggregations()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"From\":\"2024-01-01T00:00:00.0000000Z\",\"To\":\"2024-01-02T00:00:00.0000000Z\",\"Max\":[3,4],\"Count\":[2,2]}", "aggregation"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<TimeSeriesRangeAggregation<StockPrice>>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<TimeSeriesRangeAggregation<StockPrice>>(json);

            Assert.Equal(4, actual.Max.Close);
            Assert.Equal(expected.Max.Close, actual.Max.Close);
            Assert.Equal(expected.Count.Open, actual.Count.Open);
            Assert.Equal(expected.From, actual.From);
            Assert.Equal(expected.To, actual.To);
        }
    }

    private class CallbacksBase
    {
        [JsonIgnore]
        public List<string> Calls { get; } = [];

        [OnDeserialized]
        private void OnBaseDeserialized(StreamingContext context) => Calls.Add("base-deserialized");
    }

    private class WithCallbacks : CallbacksBase
    {
        public string Name { get; set; }

        [OnSerializing]
        private void OnSerializingMethod(StreamingContext context) => Name = Name?.Trim();

        [OnDeserializing]
        private void OnDeserializingMethod(StreamingContext context) => Calls.Add("deserializing");

        [OnDeserialized]
        private void OnDeserializedMethod(StreamingContext context) => Calls.Add("deserialized");
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRunsSerializationCallbacksLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new WithCallbacks { Name = "  x  " }, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new WithCallbacks { Name = "  x  " }, context))
        {
            Assert.Equal(expected.ToString(), actual.ToString());
            Assert.True(actual.TryGet(nameof(WithCallbacks.Name), out string name));
            Assert.Equal("x", name);

            var newtonsoftRead = Newtonsoft.Serialization.DefaultConverter.FromBlittable<WithCallbacks>(expected);
            var systemTextJsonRead = SystemTextJson.Serialization.DefaultConverter.FromBlittable<WithCallbacks>(expected);
            Assert.Equal(["deserializing", "base-deserialized", "deserialized"], newtonsoftRead.Calls);
            Assert.Equal(newtonsoftRead.Calls, systemTextJsonRead.Calls);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
    public void SystemTextJsonReadsTypedTimeSeriesEntries()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"Timestamp\":\"2024-01-01T00:00:00.0000000Z\",\"Values\":[1,2],\"Tag\":\"t\",\"IsRollup\":false}", "entry"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<TimeSeriesEntry<StockPrice>>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<TimeSeriesEntry<StockPrice>>(json);

            Assert.Equal(2, actual.Value.Close);
            Assert.Equal(expected.Value.Open, actual.Value.Open);
            Assert.Equal(expected.Value.Close, actual.Value.Close);
            Assert.Equal(expected.Tag, actual.Tag);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsExpandoObjectsLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"I\":1,\"D\":1.5,\"S\":\"x\",\"B\":true,\"N\":null,\"O\":{\"A\":1,\"L\":[2]},\"L\":[1,\"a\",{\"B\":2},[3]]}", "expando"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<ExpandoObject>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<ExpandoObject>(json);

            AssertSameValue(expected, actual, "$");
        }

        static void AssertSameValue(object expected, object actual, string path)
        {
            Assert.True(expected?.GetType() == actual?.GetType(), $"{path}: expected {expected?.GetType()} but got {actual?.GetType()}");

            switch (expected)
            {
                case IDictionary<string, object> expectedDictionary:
                    var actualDictionary = (IDictionary<string, object>)actual;
                    Assert.Equal(expectedDictionary.Keys.Order(), actualDictionary.Keys.Order());
                    foreach (KeyValuePair<string, object> entry in expectedDictionary)
                        AssertSameValue(entry.Value, actualDictionary[entry.Key], $"{path}.{entry.Key}");
                    break;
                case List<object> expectedList:
                    var actualList = (List<object>)actual;
                    Assert.Equal(expectedList.Count, actualList.Count);
                    for (int i = 0; i < expectedList.Count; i++)
                        AssertSameValue(expectedList[i], actualList[i], $"{path}[{i}]");
                    break;
                default:
                    Assert.True(Equals(expected, actual), $"{path}: expected {expected} but got {actual}");
                    break;
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesExpandoObjectsLikeNewtonsoft()
    {
        dynamic expando = new ExpandoObject();
        expando.Name = "x";
        expando.Count = 2;
        expando.Nested = new ExpandoObject();
        expando.Nested.City = "Hadera";
        expando.Holder = new Untyped { Expando = new ExpandoObject(), Data = new ExpandoObject() };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable((ExpandoObject)expando, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable((ExpandoObject)expando, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void PopulateEntityRefreshesDictionaryEntities(bool useSystemTextJson)
    {
        DocumentConventions conventions = useSystemTextJson ? SystemTextJson : Newtonsoft;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Name\":\"new\",\"List\":[1,2]}", "populate"))
        {
            dynamic expando = new ExpandoObject();
            expando.Name = "old";
            expando.Kept = "kept";
            conventions.Serialization.DefaultConverter.PopulateEntity((ExpandoObject)expando, json);

            var expandoValues = (IDictionary<string, object>)expando;
            Assert.Equal("new", expandoValues["Name"].ToString());
            Assert.Equal("kept", expandoValues["Kept"]);
            Assert.Equal(2, ((IEnumerable<object>)expandoValues["List"]).Count());

            var dictionary = new Dictionary<string, object> { ["Name"] = "old", ["Kept"] = "kept" };
            conventions.Serialization.DefaultConverter.PopulateEntity(dictionary, json);

            Assert.Equal("new", dictionary["Name"].ToString());
            Assert.Equal("kept", dictionary["Kept"]);
        }
    }

    private class NullHandling
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public Person Skipped { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string SkippedText { get; set; }

        public Person Kept { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonHonorsNewtonsoftNullValueHandlingLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertSameJson(new NullHandling());
            AssertSameJson(new NullHandling { Skipped = new Person { Name = "Oren" }, SkippedText = "text" });

            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"SkippedText\":null,\"Kept\":null}", "nulls"))
            {
                var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<NullHandling>(json);
                var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<NullHandling>(json);
                Assert.Equal(expected.SkippedText, actual.SkippedText);
                Assert.Equal(expected.Kept, actual.Kept);
            }

            void AssertSameJson(NullHandling value)
            {
                using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
                using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
                {
                    JObject expectedJson = JObject.Parse(expected.ToString());
                    JObject actualJson = JObject.Parse(actual.ToString());
                    Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonKeepsCharactersOutsideTheBasicMultilingualPlane()
    {
        string goblin = char.ConvertFromUtf32(0x1F47A);
        var person = new Person { Name = goblin + " goblin \"quoted\" \\ " + goblin };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = SystemTextJson.Serialization.DefaultConverter.ToBlittable(person, context))
        {
            Assert.True(json.TryGet(nameof(Person.Name), out string name));
            Assert.Equal(person.Name, name);
            Assert.Equal(person.Name, SystemTextJson.Serialization.DefaultConverter.FromBlittable<Person>(json).Name);
        }
    }

    private class Location
    {
        public Location(double lat, double lng)
        {
            Latitude = lat;
            Longitude = lng;
        }

        public double Latitude { get; set; }

        public double Longitude { get; set; }

        public string Label { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsThroughConstructorsWithUnboundParametersLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Latitude\":1.5,\"Longitude\":2.5,\"Label\":\"home\"}", "location"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<Location>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Location>(json);

            Assert.Equal(1.5, actual.Latitude);
            Assert.Equal(expected.Latitude, actual.Latitude);
            Assert.Equal(expected.Longitude, actual.Longitude);
            Assert.Equal(expected.Label, actual.Label);
        }
    }

    private class Child
    {
        public string Id { get; set; }

        public string Name { get; set; }
    }

    private class Parent
    {
        public List<Child> Children { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSetsNestedIdentitiesFromMetadataLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"Children\":[{\"Name\":\"x\",\"@metadata\":{\"@id\":\"children/1\",\"@collection\":\"Children\"}},{\"Name\":\"y\",\"@id\":\"children/2\"}]}", "nested"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<Parent>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Parent>(json);

            Assert.Equal("children/1", actual.Children[0].Id);
            Assert.Equal(expected.Children[0].Id, actual.Children[0].Id);
            Assert.Equal(expected.Children[1].Id, actual.Children[1].Id);
            Assert.Equal(expected.Children[1].Name, actual.Children[1].Name);
        }

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject written = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new Child { Id = "children/1", Name = "x" }, context))
        {
            Assert.False(written.TryGet(Raven.Client.Constants.Documents.Metadata.Key, out object _));
            Assert.False(written.TryGet(Raven.Client.Constants.Documents.Metadata.Id, out object _));
        }
    }

    private class Texts
    {
        public string Integer { get; set; }

        public string Fraction { get; set; }

        public string Integral { get; set; }

        public string Flag { get; set; }

        public string Text { get; set; }

        public Dictionary<string, string> ByKey { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsNumbersAndBooleansIntoStringsLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"Integer\":1234,\"Fraction\":1.5,\"Integral\":2.0,\"Flag\":true,\"Text\":\"text\",\"ByKey\":{\"k\":\"v\"}}", "texts"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<Texts>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Texts>(json);

            Assert.Equal("1234", actual.Integer);
            Assert.Equal(expected.Integer, actual.Integer);
            Assert.Equal(expected.Fraction, actual.Fraction);
            Assert.Equal(expected.Integral, actual.Integral);
            Assert.Equal(expected.Flag, actual.Flag);
            Assert.Equal(expected.Text, actual.Text);
            Assert.Equal(expected.ByKey, actual.ByKey);

            using (BlittableJsonReaderObject written = SystemTextJson.Serialization.DefaultConverter.ToBlittable(actual, context))
            using (BlittableJsonReaderObject newtonsoftWritten = Newtonsoft.Serialization.DefaultConverter.ToBlittable(expected, context))
                Assert.Equal(newtonsoftWritten.ToString(), written.ToString());
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
    public void SystemTextJsonWritesQueryParametersLikeNewtonsoft()
    {
        var parameters = new Raven.Client.Parameters
        {
            ["typed"] = new[] { new Person { Name = "Oren", Age = 40 } },
            ["untyped"] = new List<object> { new Person { Name = "Oren", Age = 40 }, "text", 3 },
            ["single"] = new Person { Name = "Oren", Age = 40 }
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(parameters, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(parameters, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());

            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
        }
    }

    private class Flags
    {
        public bool FromString { get; set; }

        public bool FromUpperCaseString { get; set; }

        public bool FromNumber { get; set; }

        public bool? Optional { get; set; }

        public Dictionary<bool, int> ByFlag { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsBooleansLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                   "{\"FromString\":\"true\",\"FromUpperCaseString\":\"False\",\"FromNumber\":1,\"Optional\":\"true\",\"ByFlag\":{\"True\":1,\"False\":0}}", "flags"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<Flags>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<Flags>(json);

            Assert.True(actual.FromString);
            Assert.Equal(expected.FromString, actual.FromString);
            Assert.Equal(expected.FromUpperCaseString, actual.FromUpperCaseString);
            Assert.Equal(expected.FromNumber, actual.FromNumber);
            Assert.Equal(expected.Optional, actual.Optional);
            Assert.Equal(expected.ByFlag, actual.ByFlag);

            using (BlittableJsonReaderObject newtonsoftWritten = Newtonsoft.Serialization.DefaultConverter.ToBlittable(expected, context))
            using (BlittableJsonReaderObject written = SystemTextJson.Serialization.DefaultConverter.ToBlittable(actual, context))
                Assert.Equal(newtonsoftWritten.ToString(), written.ToString());
        }
    }

    private class OrderBase
    {
        public string BaseProperty { get; set; } = "base-property";

        public string BaseField = "base-field";
    }

    private class OrderDerived : OrderBase
    {
        public string Answer = "answer";

        public List<string> Ids { get; set; } = ["id"];

        public string Name { get; set; } = "name";

        public string Tail = "tail";
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesMembersInNewtonsoftOrder()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new OrderDerived(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new OrderDerived(), context))
        {
            Assert.Equal(expected.ToString(), actual.ToString());
            Assert.Equal(expected.GetPropertyNames(), actual.GetPropertyNames());
        }
    }

    private class HoldsChars
    {
        public List<char> Chars { get; set; }

        public char Single { get; set; }

        public char? Optional { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsCharsLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Chars\":[0,104,\"i\"],\"Single\":105,\"Optional\":\"x\"}", "chars"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsChars>(json);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsChars>(json);

            Assert.Equal(['\0', 'h', 'i'], expected.Chars);
            Assert.Equal(expected.Chars, actual.Chars);
            Assert.Equal(expected.Single, actual.Single);
            Assert.Equal(expected.Optional, actual.Optional);
        }
    }

    private class IgnoredMembers
    {
        [IgnoreDataMember]
        public string Computed { get; set; } = "computed";

        [IgnoreDataMember]
        public Func<string> Factory { get; set; } = () => "factory";

        [NonSerialized]
        public string Transient = "transient";

        [IgnoreDataMember]
        private string PrivateIgnored { get; set; } = "private";

        public string Kept { get; set; } = "kept";

        public string GetPrivateIgnored() => PrivateIgnored;
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSkipsIgnoreDataMemberAndNonSerializedMembersLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new IgnoredMembers(), context))
            using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new IgnoredMembers(), context))
            {
                Assert.Equal(expected.ToString(), actual.ToString());
                Assert.Equal([nameof(IgnoredMembers.Kept)], actual.GetPropertyNames());
            }

            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Computed\":\"json\",\"Transient\":\"json\",\"PrivateIgnored\":\"json\",\"Kept\":\"json\"}", "ignored"))
            {
                var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<IgnoredMembers>(json);
                var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<IgnoredMembers>(json);

                Assert.Equal("computed", actual.Computed);
                Assert.Equal(expected.Computed, actual.Computed);
                Assert.Equal(expected.Transient, actual.Transient);
                Assert.Equal(expected.GetPrivateIgnored(), actual.GetPrivateIgnored());
                Assert.Equal(expected.Kept, actual.Kept);
            }
        }
    }

    [DataContract]
    private class OptInBase
    {
        [DataMember]
        public string BaseMember { get; set; } = "base member";

        public string BaseOutside { get; set; } = "base outside";
    }

    private class OptInDerived : OptInBase
    {
        public string Id { get; set; }

        [DataMember]
        public string Member { get; set; } = "member";

        [JsonProperty]
        public string JsonPropertyMember { get; set; } = "json property";

        [JsonRequired]
        public string Required { get; set; } = "required";

        public string Outside { get; set; } = "outside";

        [DataMember]
        private string PrivateMember { get; set; } = "private member";

        private string PrivateOutside { get; set; } = "private outside";

        public string Field = "field";

        public string GetPrivateMember() => PrivateMember;
    }

    private class NoContractBase
    {
        [DataMember]
        public string InheritedMember { get; set; } = "inherited member";
    }

    [DataContract]
    private class ContractOverNoContractBase : NoContractBase
    {
        [DataMember]
        public string Member { get; set; } = "member";
    }

    [JsonObject(MemberSerialization.OptIn)]
    private class JsonObjectOptIn
    {
        [JsonProperty]
        public string Member { get; set; } = "member";

        [DataMember]
        public string OutsideContract { get; set; } = "outside contract";

        public string Outside { get; set; } = "outside";
    }

    [JsonObject(MemberSerialization.Fields)]
    private class JsonObjectFields
    {
        public string Field = "field";

        public string Property { get; set; } = "property";
    }

    [JsonObject]
    [DataContract]
    private class JsonObjectOptOut
    {
        public string Outside { get; set; } = "outside";
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSerializesOnlyOptedInMembersLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertSameJson(new OptInDerived { Id = "items/1" }, ["BaseMember", "JsonPropertyMember", "Member", "PrivateMember", "Required"]);
            AssertSameJson(new ContractOverNoContractBase(), [nameof(ContractOverNoContractBase.Member)]);
            AssertSameJson(new JsonObjectOptIn(), [nameof(JsonObjectOptIn.Member)]);
            AssertSameJson(new JsonObjectFields(), [nameof(JsonObjectFields.Field)]);
            AssertSameJson(new JsonObjectOptOut(), [nameof(JsonObjectOptOut.Outside)]);

            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                       "{\"BaseMember\":\"json\",\"BaseOutside\":\"json\",\"Member\":\"json\",\"Outside\":\"json\",\"PrivateMember\":\"json\",\"PrivateOutside\":\"json\",\"Field\":\"json\",\"Required\":\"json\"}", "opt-in"))
            {
                var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<OptInDerived>(json);
                var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<OptInDerived>(json);

                Assert.Equal("json", actual.Member);
                Assert.Equal("outside", actual.Outside);
                Assert.Equal(expected.BaseMember, actual.BaseMember);
                Assert.Equal(expected.BaseOutside, actual.BaseOutside);
                Assert.Equal(expected.Member, actual.Member);
                Assert.Equal(expected.Outside, actual.Outside);
                Assert.Equal(expected.GetPrivateMember(), actual.GetPrivateMember());
                Assert.Equal(expected.Field, actual.Field);
                Assert.Equal(expected.Required, actual.Required);
            }

            void AssertSameJson(object value, string[] members)
            {
                using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
                using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
                {
                    JObject expectedJson = JObject.Parse(expected.ToString());
                    JObject actualJson = JObject.Parse(actual.ToString());
                    Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
                    Assert.Equal(members, actual.GetPropertyNames().Order());
                }
            }
        }
    }

    [DataContract]
    private class DefaultValues
    {
        [DataMember(EmitDefaultValue = false)]
        public int Count { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public string Text { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public int? Optional { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public Guid Key { get; set; }

        [DataMember]
        public int Emitted { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public int Initialized { get; set; } = 5;

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        [DefaultValue(10)]
        public long Limit { get; set; } = 7;

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public double Ratio { get; set; } = 1.5;

        [JsonProperty]
        [DataMember(EmitDefaultValue = false)]
        public int JsonPropertyWins { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSkipsDefaultValuesLikeNewtonsoft()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertSameJson(new DefaultValues(), ["Emitted", "Initialized", "JsonPropertyWins", "Limit", "Ratio"]);
            AssertSameJson(new DefaultValues { Count = 1, Text = "text", Optional = 0, Key = Guid.NewGuid(), Initialized = 0, Limit = 10, Ratio = 0 },
                ["Count", "Emitted", "JsonPropertyWins", "Key", "Optional", "Text"]);

            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Count\":0,\"Initialized\":0,\"Limit\":10,\"Ratio\":0,\"Text\":null}", "defaults"))
            {
                var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<DefaultValues>(json);
                var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<DefaultValues>(json);

                Assert.Equal(5, actual.Initialized);
                Assert.Equal(7, actual.Limit);
                Assert.Equal(expected.Count, actual.Count);
                Assert.Equal(expected.Initialized, actual.Initialized);
                Assert.Equal(expected.Limit, actual.Limit);
                Assert.Equal(expected.Ratio, actual.Ratio);
                Assert.Equal(expected.Text, actual.Text);
            }

            void AssertSameJson(DefaultValues value, string[] members)
            {
                using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
                using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
                {
                    JObject expectedJson = JObject.Parse(expected.ToString());
                    JObject actualJson = JObject.Parse(actual.ToString());
                    Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
                    Assert.Equal(members, actual.GetPropertyNames().Order());
                }
            }
        }
    }

    [DataContract]
    private class OrderedByAttributes
    {
        [DataMember]
        public string Unordered { get; set; } = "unordered";

        [DataMember(Order = 2)]
        public string Second { get; set; } = "second";

        [JsonProperty(Order = 1)]
        public string First { get; set; } = "first";

        [JsonProperty(Order = -2)]
        public string BeforeUnordered { get; set; } = "before unordered";

        [JsonProperty]
        [DataMember(Order = 3)]
        public string JsonPropertyWins { get; set; } = "json property";
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonOrdersMembersByNewtonsoftOrderAttributes()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new OrderedByAttributes(), context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new OrderedByAttributes(), context))
        {
            Assert.Equal(expected.ToString(), actual.ToString());
            Assert.Equal(["BeforeUnordered", "Unordered", "JsonPropertyWins", "First", "Second"], JObject.Parse(actual.ToString()).Properties().Select(x => x.Name));
        }
    }

    [JsonObject(IsReference = true)]
    private class ReferencedByJsonObject
    {
        public string Name { get; set; }
    }

    [DataContract(IsReference = true)]
    private class ReferencedByDataContract
    {
        [DataMember]
        public string Name { get; set; }
    }

    [JsonObject(IsReference = false)]
    [DataContract(IsReference = true)]
    private class ContainerOverridesDataContract
    {
        [DataMember]
        public string Name { get; set; }
    }

    private class HoldsReferencedType
    {
        public ReferencedByJsonObject Value { get; set; }
    }

    private class MemberPreservesReferences
    {
        [JsonProperty(IsReference = true)]
        public Person Owner { get; set; }
    }

    [JsonDictionary(IsReference = true)]
    private class ReferencedDictionary : Dictionary<string, Person>
    {
    }

    [JsonArray(IsReference = true)]
    private class ReferencedList : List<Person>
    {
    }

    private class ArraysIgnoreReferences
    {
        [JsonProperty(ItemIsReference = true)]
        public List<Person> People { get; set; }

        [JsonProperty(IsReference = true)]
        public Person[] Array { get; set; }

        public ReferencedList List { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsNewtonsoftReferencePreservation()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertRejected(new ReferencedByJsonObject { Name = "x" }, nameof(ReferencedByJsonObject));
            AssertRejected(new ReferencedByDataContract { Name = "x" }, nameof(ReferencedByDataContract));
            AssertRejected(new HoldsReferencedType { Value = new ReferencedByJsonObject { Name = "x" } }, nameof(ReferencedByJsonObject));
            AssertRejected(new MemberPreservesReferences { Owner = new Person { Name = "x" } }, nameof(MemberPreservesReferences.Owner));
            AssertRejected(new ReferencedDictionary { ["x"] = new Person { Name = "x" } }, nameof(ReferencedDictionary));

            var person = new Person { Name = "x" };
            AssertAccepted(new ContainerOverridesDataContract { Name = "x" });
            AssertAccepted(new ArraysIgnoreReferences { People = [person, person], Array = [person], List = [person] });

            void AssertAccepted(object entity)
            {
                using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(entity, context))
                using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context))
                {
                    Assert.DoesNotContain("\"$id\"", expected.ToString());
                    Assert.Equal(expected.ToString(), actual.ToString());
                }
            }

            void AssertRejected(object entity, string target)
            {
                using (BlittableJsonReaderObject written = Newtonsoft.Serialization.DefaultConverter.ToBlittable(entity, context))
                    Assert.Contains("\"$id\"", written.ToString());

                var exception = Assert.Throws<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context));
                Assert.Contains(target, exception.Message);
                Assert.Contains("IsReference", exception.Message);
            }
        }
    }

    [JsonObject(NamingStrategyType = typeof(Newtonsoft.Json.Serialization.CamelCaseNamingStrategy))]
    private class CamelCaseObject
    {
        public string FirstName { get; set; }
    }

    private class CamelCaseMember
    {
        [JsonProperty(NamingStrategyType = typeof(Newtonsoft.Json.Serialization.CamelCaseNamingStrategy))]
        public string FirstName { get; set; }
    }

    [JsonDictionary(NamingStrategyType = typeof(Newtonsoft.Json.Serialization.CamelCaseNamingStrategy), NamingStrategyParameters = new object[] { true, false })]
    private class CamelCaseDictionary : Dictionary<string, string>
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsNewtonsoftNamingStrategies()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertRejected(new CamelCaseObject { FirstName = "x" }, nameof(CamelCaseObject));
            AssertRejected(new CamelCaseMember { FirstName = "x" }, nameof(CamelCaseMember.FirstName));
            AssertRejected(new CamelCaseDictionary { ["FirstName"] = "x" }, nameof(CamelCaseDictionary));

            void AssertRejected(object entity, string target)
            {
                using (BlittableJsonReaderObject written = Newtonsoft.Serialization.DefaultConverter.ToBlittable(entity, context))
                    Assert.Contains("\"firstName\"", written.ToString());

                var exception = Assert.Throws<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context));
                Assert.Contains(target, exception.Message);
                Assert.Contains("NamingStrategyType", exception.Message);
            }
        }
    }

    private enum Tone
    {
        Warm,
        Cool
    }

    [JsonArray(ItemConverterType = typeof(UnixDateTimeConverter))]
    private class UnixTimestamps : List<DateTime>
    {
    }

    private class HoldsUnixTimestamps
    {
        public UnixTimestamps Timestamps { get; set; }
    }

    private class MemberItemConverter
    {
        [JsonProperty(ItemConverterType = typeof(UnixDateTimeConverter))]
        public Dictionary<string, DateTime> Timestamps { get; set; }
    }

    private class TranslatedItemConverters
    {
        [JsonProperty(ItemConverterType = typeof(StringEnumConverter))]
        public List<Tone> Tones { get; set; }

        [JsonProperty(ItemConverterType = typeof(StringEnumConverter))]
        public Dictionary<string, Tone?> TonesByName { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsUntranslatableNewtonsoftItemConverters()
    {
        var timestamp = new DateTime(2026, 10, 29, 0, 0, 0, DateTimeKind.Utc);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            AssertRejected(new HoldsUnixTimestamps { Timestamps = [timestamp] }, nameof(UnixTimestamps));
            AssertRejected(new MemberItemConverter { Timestamps = new Dictionary<string, DateTime> { ["x"] = timestamp } }, nameof(MemberItemConverter.Timestamps));

            var translated = new TranslatedItemConverters { Tones = [Tone.Cool], TonesByName = new Dictionary<string, Tone?> { ["x"] = Tone.Warm } };
            using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(translated, context))
            using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(translated, context))
            {
                Assert.Equal(expected.ToString(), actual.ToString());
                Assert.Contains("\"Cool\"", actual.ToString());
            }

            void AssertRejected(object entity, string target)
            {
                using (BlittableJsonReaderObject written = Newtonsoft.Serialization.DefaultConverter.ToBlittable(entity, context))
                    Assert.Contains("1793232000", written.ToString());

                var exception = Assert.Throws<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(entity, context));
                Assert.Contains(target, exception.Message);
                Assert.Contains("ItemConverterType", exception.Message);
            }
        }
    }

    private class HoldsException
    {
        public Exception Error;
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonSkipsExceptionTargetSiteLikeNewtonsoft()
    {
        Exception error;
        try
        {
            throw new InvalidOperationException("failed");
        }
        catch (Exception e)
        {
            error = e;
        }

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new HoldsException { Error = error }, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new HoldsException { Error = error }, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
            Assert.Null(actualJson["Error"]["TargetSite"]);
            Assert.Equal("failed", (string)actualJson["Error"]["Message"]);
        }
    }

    private class HoldsUntypedDictionaries
    {
        public Hashtable Table { get; set; }

        public IDictionary Untyped { get; set; }

        public Dictionary<object, object> ObjectKeys { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesAndReadsNonGenericDictionariesLikeNewtonsoft()
    {
        var value = new HoldsUntypedDictionaries
        {
            Table = new Hashtable { ["text"] = "value", ["person"] = new Person { Name = "Oren" } },
            Untyped = new SortedList { ["person"] = new Person { Name = "Ayende" } },
            ObjectKeys = new Dictionary<object, object> { [1] = "one", ["two"] = new Person { Name = "Two" } }
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            var expectedRead = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsUntypedDictionaries>(expected);
            var actualRead = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsUntypedDictionaries>(expected);

            Assert.Equal("value", actualRead.Table["text"]);
            Assert.Equal("Oren", Assert.IsType<Person>(actualRead.Table["person"]).Name);
            Assert.Equal(expectedRead.Untyped.GetType(), actualRead.Untyped.GetType());
            Assert.Equal("Ayende", Assert.IsType<Person>(actualRead.Untyped["person"]).Name);
            Assert.Equal(expectedRead.ObjectKeys.Keys.OrderBy(k => k.ToString()), actualRead.ObjectKeys.Keys.OrderBy(k => k.ToString()));
            Assert.Equal("Two", Assert.IsType<Person>(actualRead.ObjectKeys["two"]).Name);
        }
    }

    [TypeConverter(typeof(OrderIdConverter))]
    private readonly struct OrderId(string value)
    {
        public string Value { get; } = value;
    }

    private sealed class OrderIdConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType) => sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType) => destinationType == typeof(string) || base.CanConvertTo(context, destinationType);

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value) => value is string text ? new OrderId(text) : base.ConvertFrom(context, culture, value);

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType) =>
            destinationType == typeof(string) && value is OrderId orderId ? orderId.Value : base.ConvertTo(context, culture, value, destinationType);
    }

    private class HoldsStringContracts
    {
        public CultureInfo Culture { get; set; }

        public Type Type { get; set; }

        public OrderId OrderId { get; set; }

        public OrderId? OptionalOrderId { get; set; }

        public Dictionary<OrderId, string> ByOrderId { get; set; }

        public object Untyped { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesTypesWithStringConvertersAsStringsLikeNewtonsoft()
    {
        var value = new HoldsStringContracts
        {
            Culture = new CultureInfo("fr-CA"),
            Type = typeof(Person),
            OrderId = new OrderId("orders/1"),
            OptionalOrderId = new OrderId("orders/2"),
            ByOrderId = new Dictionary<OrderId, string> { [new OrderId("orders/3")] = "third" },
            Untyped = new CultureInfo("en-US")
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");
            Assert.Equal("fr-CA", (string)actualJson[nameof(HoldsStringContracts.Culture)]);
            Assert.Equal("orders/1", (string)actualJson[nameof(HoldsStringContracts.OrderId)]);

            var read = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsStringContracts>(expected);
            Assert.Equal("fr-CA", read.Culture.Name);
            Assert.Equal(typeof(Person), read.Type);
            Assert.Equal("orders/1", read.OrderId.Value);
            Assert.Equal("orders/2", read.OptionalOrderId?.Value);
            Assert.Equal("third", read.ByOrderId[new OrderId("orders/3")]);
            Assert.Equal("en-US", read.Untyped?.ToString());
        }
    }

    private struct Coordinates
    {
        public int X { get; set; }

        public int Y { get; set; }
    }

    private class HoldsNullableStruct
    {
        public Coordinates? Location { get; set; }

        public Coordinates? Missing { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonWritesAndReadsNullableStructsLikeNewtonsoft()
    {
        var read = AssertWritesLikeNewtonsoftAndReadsNewtonsoftJson(new HoldsNullableStruct { Location = new Coordinates { X = 1, Y = 2 } });

        Assert.Equal(2, read.Location?.Y);
        Assert.Null(read.Missing);
    }

    private class HoldsReadOnlyCollections
    {
        public ReadOnlyCollection<int> Numbers { get; set; }

        public ReadOnlyCollection<string> Names { get; set; }

        public ReadOnlyCollection<Animal> Animals { get; set; }

        public ReadOnlyDictionary<string, int> Counts { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonReadsReadOnlyCollectionsLikeNewtonsoft()
    {
        var read = AssertWritesLikeNewtonsoftAndReadsNewtonsoftJson(new HoldsReadOnlyCollections
        {
            Numbers = new ReadOnlyCollection<int>([1, 2]),
            Names = new ReadOnlyCollection<string>(["a"]),
            Animals = new ReadOnlyCollection<Animal>([new Animal { Name = "animal" }, new Dog { Name = "dog", Barks = true }]),
            Counts = new ReadOnlyDictionary<string, int>(new Dictionary<string, int> { ["a"] = 1 })
        });

        Assert.Equal([1, 2], read.Numbers);
        Assert.Equal(["a"], read.Names);
        Assert.IsType<Dog>(read.Animals[1]);
        Assert.Equal(1, read.Counts["a"]);
    }

    private class HoldsDates
    {
        public DateTime When { get; set; }

        public DateTime? MaybeWhen { get; set; }

        public DateTimeOffset At { get; set; }

        public DateTimeOffset? MaybeAt { get; set; }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(nameof(HoldsDates.When))]
    [InlineData(nameof(HoldsDates.MaybeWhen))]
    [InlineData(nameof(HoldsDates.At))]
    [InlineData(nameof(HoldsDates.MaybeAt))]
    public void SystemTextJsonRejectsUnparseableDatesLikeNewtonsoft(string member)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory($"{{\"{member}\":\"2026-01-02\"}}", "dates"))
        {
            Assert.ThrowsAny<Exception>(() => Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsDates>(json));

            var exception = Assert.ThrowsAny<Exception>(() => SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsDates>(json));
            Assert.Contains("2026-01-02", exception.ToString());
        }
    }

    private class HoldsBigInteger
    {
        public System.Numerics.BigInteger Value { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void SystemTextJsonRejectsBigIntegerWritesAndReadsBigIntegerValuesLikeNewtonsoft()
    {
        var value = new HoldsBigInteger { Value = System.Numerics.BigInteger.Parse("12345678901234567890123") };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            Assert.ThrowsAny<Exception>(() => Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context));

            var exception = Assert.ThrowsAny<NotSupportedException>(() => SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context));
            Assert.Contains(nameof(System.Numerics.BigInteger), exception.Message);

            foreach (string json in new[] { "{\"Value\":12345678901234567890123}", "{\"Value\":\"12345678901234567890123\"}" })
            {
                using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory(json, "big-integer"))
                {
                    var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);
                    var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);

                    Assert.Equal(value.Value, expected.Value);
                    Assert.Equal(expected.Value, actual.Value);
                }
            }
        }
    }

    private static T AssertWritesLikeNewtonsoftAndReadsNewtonsoftJson<T>(T value)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            return SystemTextJson.Serialization.DefaultConverter.FromBlittable<T>(expected);
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
