using System;
using System.Collections.Generic;
using System.Dynamic;
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
