using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class PatchEnumMemberValuesTests(ITestOutputHelper output) : RavenTestBase(output)
{
    public enum Stage
    {
        [EnumMember(Value = "not-started")]
        NotStarted,

        [EnumMember(Value = "in-progress")]
        InProgress,

        Done
    }

    public class Item
    {
        public Stage Stage { get; set; }

        public Stage? MaybeStage { get; set; }

        public List<Stage> History { get; set; }

        public Dictionary<string, Stage> ByStep { get; set; }
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData(false, SessionPatchBehavior.JavaScript)]
    [InlineData(true, SessionPatchBehavior.JavaScript)]
    [InlineData(false, SessionPatchBehavior.JsonPatch)]
    [InlineData(true, SessionPatchBehavior.JsonPatch)]
    public async Task SessionPatchesStoreEnumMemberValuesLikeStore(bool useSystemTextJson, SessionPatchBehavior behavior)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.Conventions.Serialization = useSystemTextJson ? new SystemTextJsonSerializationConventions() : new NewtonsoftJsonSerializationConventions();
                s.Conventions.SessionPatchBehavior = behavior;
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(CreateItem(), "items/1");
            await session.StoreAsync(CreateItem(), "items/2");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Patch<Item, Stage>("items/1", x => x.Stage, Stage.InProgress);
            session.Advanced.Patch<Item, Stage?>("items/1", x => x.MaybeStage, Stage.InProgress);
            session.Advanced.Patch<Item, Stage>("items/1", x => x.History, history => history.Add(Stage.InProgress));
            session.Advanced.Patch<Item, string, Stage>("items/1", x => x.ByStep, steps => steps.Add("review", Stage.InProgress));
            session.Advanced.AddOrPatch("items/2", CreateItem(), x => x.Stage, Stage.InProgress);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            BlittableJsonReaderObject item = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
            Assert.Equal("in-progress", item[nameof(Item.Stage)].ToString());
            Assert.Equal("in-progress", item[nameof(Item.MaybeStage)].ToString());
            Assert.Equal(["not-started", "in-progress"], ((BlittableJsonReaderArray)item[nameof(Item.History)]).Select(x => x.ToString()));

            var byStep = (BlittableJsonReaderObject)item[nameof(Item.ByStep)];
            Assert.Equal("not-started", byStep["draft"].ToString());
            Assert.Equal("in-progress", byStep["review"].ToString());

            BlittableJsonReaderObject added = await session.LoadAsync<BlittableJsonReaderObject>("items/2");
            Assert.Equal("in-progress", added[nameof(Item.Stage)].ToString());
        }

        using (var session = store.OpenAsyncSession())
        {
            Item item = await session.LoadAsync<Item>("items/1");
            Assert.Equal(Stage.InProgress, item.Stage);
            Assert.Equal(Stage.InProgress, item.MaybeStage);
            Assert.Equal([Stage.NotStarted, Stage.InProgress], item.History);
            Assert.Equal(Stage.InProgress, item.ByStep["review"]);
            Assert.Equal(Stage.InProgress, (await session.LoadAsync<Item>("items/2")).Stage);
            Assert.False(session.Advanced.HasChanges);
        }
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchOperationValuesStoreEnumMemberValuesLikeStore(bool useSystemTextJson)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.Conventions.Serialization = useSystemTextJson ? new SystemTextJsonSerializationConventions() : new NewtonsoftJsonSerializationConventions();
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(CreateItem(), "items/1");
            await session.SaveChangesAsync();
        }

        await store.Operations.SendAsync(new PatchOperation("items/1", null, new PatchRequest
        {
            Script = "this.Stage = args.stage; this.History = args.history;",
            Values =
            {
                ["stage"] = Stage.InProgress,
                ["history"] = new[] { Stage.NotStarted, Stage.InProgress }
            }
        }));

        using (var session = store.OpenAsyncSession())
        {
            BlittableJsonReaderObject item = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
            Assert.Equal("in-progress", item[nameof(Item.Stage)].ToString());
            Assert.Equal(["not-started", "in-progress"], ((BlittableJsonReaderArray)item[nameof(Item.History)]).Select(x => x.ToString()));
        }

        using (var session = store.OpenAsyncSession())
        {
            Item item = await session.LoadAsync<Item>("items/1");
            Assert.Equal(Stage.InProgress, item.Stage);
            Assert.Equal([Stage.NotStarted, Stage.InProgress], item.History);
        }
    }

    [RavenFact(RavenTestCategory.Patching)]
    public void PatchValuesUseTheEnumNamesOfTheirConventions()
    {
        var plain = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
        var camelCase = new DocumentConventions
        {
            Serialization = new SystemTextJsonSerializationConventions
            {
                CustomizeJsonSerializerOptions = options => options.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase))
            }
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            for (int i = 0; i < 2; i++)
            {
                Assert.Equal("in-progress", GetPatchValue(plain, Stage.InProgress, context));
                Assert.Equal("Done", GetPatchValue(plain, Stage.Done, context));
                Assert.Equal("done", GetPatchValue(camelCase, Stage.Done, context));
            }
        }
    }

    private static string GetPatchValue(DocumentConventions conventions, Stage stage, JsonOperationContext context)
    {
        var patch = new PatchRequest { Script = "this.Stage = args.stage", Values = { ["stage"] = stage } };
        using (BlittableJsonReaderObject json = context.ReadObject(patch.ToJson(conventions, context), "patch"))
        {
            Assert.True(json.TryGet(nameof(PatchRequest.Values), out BlittableJsonReaderObject values));
            Assert.True(values.TryGet("stage", out string value));
            return value;
        }
    }

    private static Item CreateItem() => new()
    {
        Stage = Stage.NotStarted,
        History = [Stage.NotStarted],
        ByStep = new Dictionary<string, Stage> { ["draft"] = Stage.NotStarted }
    };
}
