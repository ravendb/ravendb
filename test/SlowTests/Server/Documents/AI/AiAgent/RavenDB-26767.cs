using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_26767 : RavenTestBase
{
    public RavenDB_26767(ITestOutputHelper output) : base(output)
    {
    }

    // Regression coverage: the GenAI redesign must not alter the existing AI-Agent parameter wrapper contract.
    // ConversationHandler.GetAiConversationParameter still parses { Value, SendToModel } wrappers exactly as before.
    // These stay fast, offline RavenFacts because the SendToModel integration coverage (RavenDB_25186 / RavenDB_25975)
    // is credential-gated and may be skipped when provider credentials are unavailable.

    [RavenFact(RavenTestCategory.Ai)]
    public void ExistingWrapper_RespectsSendToModel()
    {
        // An explicit SendToModel=false wrapper is honored.
        using var context = JsonOperationContext.ShortTermSingleUse();
        var wrapper = context.ReadObject(new DynamicJsonValue
        {
            [nameof(AiConversationParameter.Value)] = 3500L,
            [nameof(AiConversationParameter.SendToModel)] = false
        }, "budgetNis");

        var param = ConversationHandler.GetAiConversationParameter("budgetNis", wrapper);

        Assert.False(param.SendToModel);
        Assert.Equal(3500L, param.Value);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void RawObjectContainingValue_IsStillInterpretedAsWrapper()
    {
        // A raw object with a "Value" member is read as the wrapper: Value is extracted, SendToModel defaults to
        // true when absent, and additional business fields are ignored by the legacy parser.
        using var context = JsonOperationContext.ShortTermSingleUse();
        var oldRaw = context.ReadObject(new DynamicJsonValue { ["Value"] = 100L, ["Currency"] = "USD" }, "price");

        var param = ConversationHandler.GetAiConversationParameter("price", oldRaw);

        Assert.Equal(100L, param.Value);
        Assert.True(param.SendToModel);
    }

    // === GenAI raw context (no wrapping): the full context reaches the model ONCE via the user prompt, and there is
    //     NO "AI Agent Parameters:" message. Object/nested/full-document contexts flow through unchanged. ===

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_FullDocumentContext_ReachesModelOnce_NoParametersMessage()
    {
        await SendToModelViaGenAiTask(
        [
            ("docs/1", new DynamicJsonValue
            {
                ["doc"] = new DynamicJsonValue
                {
                    ["title"] = "TechNova Phone X",
                    ["description"] = "Flagship smartphone ad",
                    ["brand"] = new DynamicJsonValue { ["name"] = "TechNova", ["category"] = "Consumer Electronics" }
                }
            })
        ], (_, payloads) =>
        {
            Assert.Single(payloads);
            var contents = GetMessageContents(payloads[0]);

            // no duplicate parameters message - GenAI does not emit "AI Agent Parameters:"
            Assert.DoesNotContain(contents, c => c.Contains("AI Agent Parameters:"));

            // the full nested document reaches the model exactly once, via the user prompt
            var withContext = contents.Where(c => c.Contains("TechNova Phone X")).ToList();
            Assert.Single(withContext);
            Assert.Contains("Consumer Electronics", withContext[0]);
            Assert.Contains("Flagship smartphone ad", withContext[0]);
        });
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_ObjectWithWrapperLikeNames_ReachesModelIntact()
    {
        // A business object literally shaped like the internal wrapper ({ Value, SendToModel }) plus an extra field
        // is ordinary model context: it must reach the model complete, with no field treated as wrapper metadata.
        await SendToModelViaGenAiTask(
        [
            ("docs/1", new DynamicJsonValue
            {
                ["item"] = new DynamicJsonValue { ["Value"] = 100L, ["SendToModel"] = false, ["Description"] = "business data" }
            })
        ], (_, payloads) =>
        {
            Assert.Single(payloads);
            var contents = GetMessageContents(payloads[0]);
            Assert.DoesNotContain(contents, c => c.Contains("AI Agent Parameters:"));

            var withContext = contents.Where(c => c.Contains("business data")).ToList();
            Assert.Single(withContext);
            Assert.Contains("\"Value\":100", withContext[0]);
            Assert.Contains("\"SendToModel\":false", withContext[0]); // carried as data, not a directive
        });
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_MultipleItems_NoValueLeakBetweenRequests()
    {
        var markers = new[] { "ALPHA", "BETA", "GAMMA", "DELTA" };
        var items = markers
            .Select((m, i) => ($"docs/{i + 1}", new DynamicJsonValue
            {
                ["doc"] = new DynamicJsonValue
                {
                    ["marker"] = m,
                    ["brand"] = new DynamicJsonValue { ["name"] = m + "Brand" }
                }
            }))
            .ToList();

        await SendToModelViaGenAiTask(items, (result, payloads) =>
        {
            Assert.Equal(markers.Length, result.Results.Count);
            Assert.Equal(markers.Length, payloads.Length);

            foreach (var marker in markers)
            {
                var matching = payloads.Where(p => p.Contains(marker)).ToList();
                Assert.Single(matching); // exactly one request carries this marker
                Assert.Contains(marker + "Brand", matching[0]);
                foreach (var other in markers.Where(m => m != marker))
                    Assert.DoesNotContain(other, matching[0]); // no leak between items
            }
        });
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_TestMode_OnlySupportedValuesFilteredRaw_ObjectsOmitted()
    {
        // Filtered raw GenAI query parameters: ConversationDocument.Parameters holds ONLY top-level context values that
        // are accepted by the AI-Agent parameter value contract (AiConversationParameterValueHelper), stored RAW (no
        // wrapper). Objects and arrays containing objects are OMITTED (they remain model context via UserPrompt). null,
        // empty arrays and homogeneous scalar arrays are supported (provided values).
        // ContextOutput.Context (Test Context output) stays raw and complete.
        await SendToModelViaGenAiTask(
        [
            ("docs/1", new DynamicJsonValue
            {
                ["companyId"] = "companies/1-A",                                             // string scalar -> raw
                ["minRam"] = 16L,                                                            // numeric scalar -> raw
                ["active"] = true,                                                           // boolean scalar -> raw
                ["ids"] = new DynamicJsonArray(new object[] { "companies/1-A" }),            // string array -> raw
                ["sizes"] = new DynamicJsonArray(new object[] { 1L, 2L }),                   // numeric array -> raw
                ["empty"] = new DynamicJsonArray(),                                          // empty array -> raw (provided)
                ["nullVal"] = null,                                                          // null -> raw
                ["doc"] = new DynamicJsonValue { ["title"] = "Phone" },                      // object -> OMITTED
                ["items"] = new DynamicJsonArray(new object[] { new DynamicJsonValue { ["Id"] = "x" } }) // array-with-object -> OMITTED
            })
        ], (result, _) =>
        {
            var item = result.Results[0];

            // Test Context output stays raw (unwrapped, complete - the object is still there)
            Assert.True(item.ContextOutput.Context.TryGet("doc", out BlittableJsonReaderObject rawDoc));
            Assert.True(rawDoc.TryGet("title", out string title));
            Assert.Equal("Phone", title);

            Assert.True(item.ModelOutput.ConversationDocument.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject p));

            // supported scalars are stored RAW (not a { Value, SendToModel } wrapper)
            Assert.True(p.TryGet("companyId", out string companyId));
            Assert.Equal("companies/1-A", companyId);
            Assert.True(p.TryGet("minRam", out long minRam));               // numeric scalar raw
            Assert.Equal(16L, minRam);
            Assert.True(p.TryGet("active", out bool active));               // boolean scalar raw
            Assert.True(active);

            // arrays stored raw
            Assert.True(p.TryGet("ids", out BlittableJsonReaderArray idsArr));
            Assert.Equal(1, idsArr.Length);
            Assert.True(p.TryGet("sizes", out BlittableJsonReaderArray sizesArr)); // numeric array raw
            Assert.Equal(2, sizesArr.Length);

            // empty array stored raw (a provided empty value, not a missing parameter)
            Assert.True(p.TryGet("empty", out BlittableJsonReaderArray emptyArr));
            Assert.Equal(0, emptyArr.Length);

            // null stored raw
            Assert.True(p.TryGetMember("nullVal", out object nullVal));
            Assert.Null(nullVal);

            // objects and arrays-containing-objects are OMITTED from Parameters
            Assert.False(p.TryGet("doc", out object _));
            Assert.False(p.TryGet("items", out object _));
        });
    }

    // === Full flow via RunTest stages (CreateContextObjects -> SendToModel -> ApplyUpdateScript):
    //     the update script receives the ORIGINAL RAW context, and the model never sees a parameters message. ===
    [RavenFact(RavenTestCategory.Ai)]
    public async Task FullFlow_OneDocument_RawContextToModelOnce_AndUpdateScriptReceivesRawInput()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        const string docId = "techdocs/1";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TechDoc { Title = "TechNova Phone", Brand = new BrandInfo { Name = "TechNova", Category = "Electronics" } }, docId);
            await session.SaveChangesAsync();
        }

        var config = new GenAiConfiguration
        {
            Name = "genai-26767-fullflow",
            Identifier = "genai-26767-fullflow",
            ConnectionStringName = "genai-cs",
            Collection = "TechDocs",
            Prompt = "You are a marketing analyst. Summarize.",
            SampleObject = "{\"Answer\":\"a\"}",
            TestMode = true,
            MaxConcurrency = 1,
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
ai.genContext({ doc: this });
ai.genContext({ title: this.Title, brand: this.Brand });
ai.genContext(this);
"
            },
            // reads the RAW $input by shape; reading a { Value, SendToModel } wrapper would yield undefined and fail below
            UpdateScript = @"
this.Results = this.Results || {};
if ($input.doc) {
    this.Results.Case1 = { Title: $input.doc.Title, Brand: $input.doc.Brand.Name };
} else if ($input.title) {
    this.Results.Case2 = { Title: $input.title, Brand: $input.brand.Name };
} else {
    this.Results.Case3 = { Title: $input.Title, Brand: $input.Brand.Name };
}
"
        };

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            using var task = new GenAiTask(config.Transforms[0], config, database, database.ServerStore);

            var payloads = new ConcurrentQueue<string>();
            using var mock = NewAnswerMock(database, payloads);
            InjectMock(task, mock);

            var created = (GenAiTestScriptResult)task.RunTest(
                new TestGenAiScript { DocumentId = docId, Configuration = config, TestStage = TestStage.CreateContextObjects }, context);
            Assert.Equal(3, created.Results.Count);

            var sent = (GenAiTestScriptResult)task.RunTest(
                new TestGenAiScript { Input = created.Results, Configuration = config, TestStage = TestStage.SendToModel }, context);

            Assert.Equal(3, payloads.Count);
            // no request carries an "AI Agent Parameters:" message; each carries its raw context once via the user prompt
            foreach (var payload in payloads)
            {
                var contents = GetMessageContents(payload);
                Assert.DoesNotContain(contents, c => c.Contains("AI Agent Parameters:"));
                Assert.Contains(contents, c => c.Contains("TechNova"));
            }

            var updated = (GenAiTestScriptResult)task.RunTest(
                new TestGenAiScript { DocumentId = docId, Input = sent.Results, Configuration = config, TestStage = TestStage.ApplyUpdateScript }, context);

            Assert.NotNull(updated.OutputDocument);
            Assert.True(updated.OutputDocument.TryGet("Results", out BlittableJsonReaderObject results));

            foreach (var caseName in new[] { "Case1", "Case2", "Case3" })
            {
                Assert.True(results.TryGet(caseName, out BlittableJsonReaderObject c),
                    $"{caseName} missing - the update script did not receive the raw context for that shape");
                Assert.True(c.TryGet("Title", out string t));
                Assert.Equal("TechNova Phone", t);
                Assert.True(c.TryGet("Brand", out string b));
                Assert.Equal("TechNova", b);
            }
        }
    }

    private sealed class TechDoc
    {
        public string Title { get; set; }
        public BrandInfo Brand { get; set; }
    }

    private sealed class BrandInfo
    {
        public string Name { get; set; }
        public string Category { get; set; }
    }

    private static List<string> GetMessageContents(string payload)
    {
        var request = JObject.Parse(payload);
        var messages = request["messages"];
        Assert.NotNull(messages);
        return messages.Select(m => m["content"]?.ToString() ?? string.Empty).ToList();
    }

    private static MockLlm NewAnswerMock(Raven.Server.Documents.DocumentDatabase database, ConcurrentQueue<string> payloads) =>
        new(database.DocumentsStorage.ContextPool,
            new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")),
            onRequest: payload =>
            {
                payloads.Enqueue(payload.ToString());
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(MockLlm.CreateAnswerResponse("\"ok\"")) };
            },
            conventions: ChatCompletionClient.ConventionsToUse);

    private static void InjectMock(GenAiTask task, MockLlm mock)
    {
        var clientField = typeof(GenAiTask).GetField("_chatCompletionClient", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(clientField); // guards against a silent NRE if the field is ever renamed
        clientField.SetValue(task, mock);
    }

    // Builds a real GenAiTask, injects a mock chat client, and drives RunTest(TestStage.SendToModel) over the given
    // context items. The inspect callback runs INSIDE the operation context and receives the result and the captured
    // outbound model-request payloads (one per item).
    private async Task SendToModelViaGenAiTask(
        List<(string docId, DynamicJsonValue context)> items,
        Action<GenAiTestScriptResult, string[]> inspect)
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var config = new GenAiConfiguration
            {
                Name = "genai-26767",
                Identifier = "genai-26767",
                ConnectionStringName = "genai-cs",
                Collection = "Docs",
                Prompt = "You are a marketing analyst. Summarize.",
                SampleObject = "{\"Answer\":\"a\"}",
                TestMode = true,
                MaxConcurrency = 1,
                GenAiTransformation = new GenAiTransformation { Script = "ai.genContext(this);" }
            };

            using var task = new GenAiTask(config.Transforms[0], config, database, database.ServerStore);

            var payloads = new ConcurrentQueue<string>();
            using var mock = NewAnswerMock(database, payloads);
            InjectMock(task, mock);

            var input = new List<GenAiResultItem>();
            foreach (var (docId, ctx) in items)
            {
                input.Add(new GenAiResultItem
                {
                    DocumentId = docId,
                    ContextOutput = new ContextOutput
                    {
                        Context = context.ReadObject(ctx, docId),
                        IsCached = false,
                        AiHash = docId,
                        Attachments = null
                    }
                });
            }

            var result = (GenAiTestScriptResult)task.RunTest(
                new TestGenAiScript { Configuration = config, TestStage = TestStage.SendToModel, Input = input }, context);
            inspect(result, payloads.ToArray());
        }
    }

    // === Query-tool binding: scalars bind and override the model; objects are model context only, never bound. ===

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_OmittedObject_NoModelArgument_ThrowsNativeMissingParameter()
    {
        // 'brand' is an object -> omitted from RequestBody.Parameters (filtered). The query references $brand and the
        // model supplies no 'brand', so RavenDB produces its NATIVE missing-parameter error - no custom exception.
        var (error, _) = await RunGenAiQueryToolAsync(
            contexts: [new DynamicJsonValue { ["brand"] = new DynamicJsonValue { ["Name"] = "TechNova" } }],
            queryRql: "from Products where Brand = $brand",
            modelToolArgs: "{}",
            seed: null);

        Assert.NotNull(error);
        Assert.Contains("was not provided", error.ToString()); // native missing-parameter error, not a custom exception
        Assert.Contains("brand", error.ToString());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_OmittedObject_ModelSuppliesScalar_QueryUsesModelValue_AcceptedTradeoff()
    {
        // ACCEPTED SEMANTIC TRADEOFF of the minimal filtered design: because the object-valued 'brand' is omitted from
        // Parameters (this design deliberately does NOT remember omitted names), the normal query-binding path leaves
        // the MODEL-supplied scalar 'brand' in place and the query completes using it. This is deliberate and is NOT
        // context-override protection - the object context simply does not participate in query binding.
        var (error, toolResults) = await RunGenAiQueryToolAsync(
            contexts: [new DynamicJsonValue { ["brand"] = new DynamicJsonValue { ["Name"] = "TechNova" } }],
            queryRql: "from Products where Brand = $brand",
            modelToolArgs: "{\"brand\":\"model-brand\"}",
            seed: s => s.Store(new Product { Brand = "model-brand" }));

        Assert.Null(error);                                            // the query completed using the model-supplied scalar
        Assert.Contains(toolResults, r => r.Contains("model-brand"));  // the model's value matched the seeded product
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_ScalarContext_OverridesModelSuppliedScalar()
    {
        // A SUPPORTED scalar context value IS wrapped and bound, overriding a competing model-supplied scalar of the
        // same name (the model cannot override a supported user context value).
        var (error, toolResults) = await RunGenAiQueryToolAsync(
            contexts: [new DynamicJsonValue { ["companyId"] = "companies/1-A" }],
            queryRql: "from Orders where CompanyId = $companyId",
            modelToolArgs: "{\"companyId\":\"companies/9-Z\"}",
            seed: s =>
            {
                s.Store(new Order { CompanyId = "companies/1-A", Note = "context-order" });
                s.Store(new Order { CompanyId = "companies/9-Z", Note = "model-order" });
            });

        Assert.Null(error);
        Assert.Contains(toolResults, r => r.Contains("context-order"));       // the context scalar was bound
        Assert.DoesNotContain(toolResults, r => r.Contains("model-order"));   // it overrode the model-supplied scalar
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_MultipleQueryInvocations_NoSourceBackedLeak()
    {
        // Two items, each binding its own scalar. Confirms the source-backed DynamicJsonValue built for one
        // invocation does not leak into the next.
        var (error, toolResults) = await RunGenAiQueryToolAsync(
            contexts:
            [
                new DynamicJsonValue { ["companyId"] = "companies/1-A" },
                new DynamicJsonValue { ["companyId"] = "companies/2-B" }
            ],
            queryRql: "from Orders where CompanyId = $companyId",
            modelToolArgs: "{}",
            seed: s =>
            {
                s.Store(new Order { CompanyId = "companies/1-A", Note = "order-A" });
                s.Store(new Order { CompanyId = "companies/2-B", Note = "order-B" });
            });

        Assert.Null(error);
        Assert.Contains(toolResults, r => r.Contains("order-A") && r.Contains("companies/1-A") && r.Contains("companies/2-B") == false);
        Assert.Contains(toolResults, r => r.Contains("order-B") && r.Contains("companies/2-B") && r.Contains("companies/1-A") == false);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_EmptyScalarArrayContext_BoundAsEmptyArray_Succeeds()
    {
        // context { ids: [] } used by `... in ($ids)`. An empty array is a supported query parameter
        // (see ValidateParameterValues, which treats a zero-length array as valid): it must be bound as an
        // empty array, so the query executes and simply matches nothing - NOT dropped with a missing-parameter error.
        var (error, toolResults) = await RunGenAiQueryToolAsync(
            contexts: [new DynamicJsonValue { ["ids"] = new DynamicJsonArray() }],
            queryRql: "from Orders where CompanyId in ($ids)",
            modelToolArgs: "{}",
            seed: s => s.Store(new Order { CompanyId = "companies/1-A", Note = "unmatched-order" }));

        Assert.Null(error);              // no "Value of parameter 'ids' was not provided"
        Assert.NotEmpty(toolResults);    // the query executed and returned a result set to the model
        // bound as an EMPTY array -> matches nothing -> the seeded order is absent from the result
        Assert.DoesNotContain(toolResults, r => r.Contains("unmatched-order"));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_NonEmptyScalarArrayContext_Binds_AndOverridesModel()
    {
        // context { ids: ["companies/1-A","companies/2-A"] } bound to `... in ($ids)`. The model supplies a different
        // ids argument; the context value must override it (model cannot override user context values).
        var (error, toolResults) = await RunGenAiQueryToolAsync(
            contexts: [new DynamicJsonValue { ["ids"] = new DynamicJsonArray(new object[] { "companies/1-A", "companies/2-A" }) }],
            queryRql: "from Orders where CompanyId in ($ids)",
            modelToolArgs: "{\"ids\":[\"companies/9-Z\"]}",
            seed: s =>
            {
                s.Store(new Order { CompanyId = "companies/1-A", Note = "context-order" });
                s.Store(new Order { CompanyId = "companies/9-Z", Note = "model-order" });
            });

        Assert.Null(error);
        Assert.Contains(toolResults, r => r.Contains("context-order"));       // the context array was bound
        Assert.DoesNotContain(toolResults, r => r.Contains("model-order"));   // the model's ids did NOT override it
    }

    // === AI-Agent parameter value classifier (ConversationHandler.TryGetValueType): the SINGLE classifier used by
    //     both ConversationHandler.ValidateParameterValues and GenAiTask.FilterSupportedGenAiQueryParameters. An empty
    //     array is accepted here (a provided empty query value with no element type to infer). ===

    [RavenFact(RavenTestCategory.Ai)]
    public void TryGetValueType_AcceptsScalarsHomogeneousArraysAndEmptyArray()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        var doc = ctx.ReadObject(new DynamicJsonValue
        {
            ["nullVal"] = null,                                                       // null
            ["str"] = "hello",                                                        // string
            ["boolean"] = true,                                                       // boolean
            ["integer"] = 42L,                                                        // integer
            ["floating"] = 3.14,                                                      // floating-point / LazyNumberValue
            ["emptyArr"] = new DynamicJsonArray(),                                    // empty array (provided empty query value)
            ["strArr"] = new DynamicJsonArray(new object[] { "a", "b" }),             // homogeneous string array
            ["numArr"] = new DynamicJsonArray(new object[] { 1L, 2L }),               // homogeneous number array
            ["boolArr"] = new DynamicJsonArray(new object[] { true, false })          // homogeneous boolean array
        }, "accepted");

        foreach (var name in new[] { "nullVal", "str", "boolean", "integer", "floating", "emptyArr", "strArr", "numArr", "boolArr" })
        {
            Assert.True(doc.TryGetMember(name, out var v));
            Assert.True(ConversationHandler.TryGetValueType(v, out _, out _), $"'{name}' should be accepted");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TryGetValueType_RejectsObjectsNestedMixedAndNullArrays()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        var doc = ctx.ReadObject(new DynamicJsonValue
        {
            ["obj"] = new DynamicJsonValue { ["x"] = 1L },                                                                   // top-level object
            ["wrapperLike"] = new DynamicJsonValue { ["Value"] = 100L, ["SendToModel"] = false },                           // object w/ Value + SendToModel
            ["arrWithObj"] = new DynamicJsonArray(new object[] { "a", new DynamicJsonValue { ["x"] = 1L } }),               // array containing an object
            ["nestedArr"] = new DynamicJsonArray(new object[] { new DynamicJsonArray(new object[] { "a" }) }),              // nested array
            ["deepNestedArr"] = new DynamicJsonArray(new object[] { new DynamicJsonArray(new object[] { new DynamicJsonArray(new object[] { "a" }) }) }), // deeply nested
            ["objInNestedArr"] = new DynamicJsonArray(new object[] { new DynamicJsonArray(new object[] { new DynamicJsonValue { ["x"] = 1L } }) }),       // object inside nested array
            ["mixedStrNum"] = new DynamicJsonArray(new object[] { "a", 1L }),                                               // mixed string/number
            ["mixedBoolNum"] = new DynamicJsonArray(new object[] { true, 1L }),                                            // mixed boolean/number
            ["nullAndStr"] = new DynamicJsonArray(new object[] { null, "a" }),                                             // null + string
            ["onlyNull"] = new DynamicJsonArray(new object[] { null })                                                     // array of only null
        }, "rejected");

        foreach (var name in new[] { "obj", "wrapperLike", "arrWithObj", "nestedArr", "deepNestedArr", "objInNestedArr", "mixedStrNum", "mixedBoolNum", "nullAndStr", "onlyNull" })
        {
            Assert.True(doc.TryGetMember(name, out var v));
            Assert.False(ConversationHandler.TryGetValueType(v, out _, out _), $"'{name}' should be rejected");
        }

        // unsupported CLR value (not blittable-sourced) -> rejected, and TryGetValueType returns false (does not throw)
        Assert.False(ConversationHandler.TryGetValueType(new object(), out _, out _));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenAi_Traced_PersistsAndReadsBackSafely_SupportedRaw_ObjectOmitted()
    {
        // Genuine end-to-end tracing: EnableTracing=true + TestMode=false drives the REAL persist path
        // (GenAiConversationHandler.TryPersistAsync -> base.TryPersistAsync -> TxMerger -> @conversations). We then read
        // it back through the REAL GetConversationMessages API (GetParameters -> GetAiConversationParameter over STORED
        // data). The supported scalar reads back; the object is ABSENT from stored Parameters; no echo; raw context once.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var config = new GenAiConfiguration
        {
            Name = "genai-26767-traced",
            Identifier = "genai-26767-traced",
            ConnectionStringName = "genai-cs",
            Collection = "Docs",
            Prompt = "Summarize.",
            SampleObject = "{\"Answer\":\"a\"}",
            TestMode = true,               // construct the task WITHOUT creating a real provider client...
            MaxConcurrency = 1,
            ExpirationInSec = 3600,
            GenAiTransformation = new GenAiTransformation { Script = "ai.genContext(this);" }
        };

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            using var task = new GenAiTask(config.Transforms[0], config, database, database.ServerStore);
            var payloads = new ConcurrentQueue<string>();
            using var mock = NewAnswerMock(database, payloads);
            InjectMock(task, mock);

            // ...then switch on the REAL persist path: tracing on, TestMode off.
            config.TestMode = false;
            config.EnableTracing = true;

            var input = new List<GenAiResultItem>
            {
                new()
                {
                    DocumentId = "docs/1",
                    ContextOutput = new ContextOutput
                    {
                        Context = context.ReadObject(new DynamicJsonValue
                        {
                            ["companyId"] = "companies/1-A",                            // scalar -> wrapped + persisted
                            ["brand"] = new DynamicJsonValue { ["Name"] = "TechNova" }  // object -> omitted from Parameters
                        }, "docs/1"),
                        IsCached = false, AiHash = "docs/1", Attachments = null
                    }
                }
            };

            task.RunTest(new TestGenAiScript { Configuration = config, TestStage = TestStage.SendToModel, Input = input }, context);
        }

        string conversationId;
        using (var session = store.OpenAsyncSession())
        {
            var persisted = await session.Advanced.LoadStartingWithAsync<PersistedConversation>($"{config.Identifier}/");
            var conv = Assert.Single(persisted);
            conversationId = session.Advanced.GetDocumentId(conv);
        }

        // read back through the REAL API over the STORED conversation (not the in-memory RunTest result)
        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions { ConversationId = conversationId, PageSize = 50 });

        // supported scalar reads back unwrapped; the object is ABSENT from stored Parameters
        Assert.NotNull(result.Parameters);
        Assert.Equal("companies/1-A", result.Parameters["companyId"]);
        Assert.False(result.Parameters.ContainsKey("brand"));

        // no "AI Agent Parameters:" message persisted; raw context (incl. the object) appears exactly once via the prompt
        Assert.DoesNotContain(result.Messages, m => (m.Content ?? string.Empty).Contains("AI Agent Parameters"));
        Assert.Single(result.Messages, m => (m.Content ?? string.Empty).Contains("companies/1-A"));
        Assert.Single(result.Messages, m => (m.Content ?? string.Empty).Contains("TechNova")); // the object still reached the model via UserPrompt
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_RealProvider_ObjectContext_CompletesWithoutError(Options options, GenAiConfiguration config)
    {
        // Credential-gated real-provider smoke test (RavenDB-26767 previously had none). Provider-stable only: a full/
        // nested object GenAI context is accepted and the ETL conversation completes end-to-end (the update script runs)
        // with no serialization / parameter-wrapper error. No model-output assertions, no tool-choice dependency.
        // Skips automatically when provider credentials are unavailable.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Identifier = "genai-26767-real";
        config.Collection = "RealProviderProducts"; // must match the collection derived from the RealProviderProduct type
        config.Prompt = "Summarize this product for marketing in one sentence.";
        config.SampleObject = JsonConvert.SerializeObject(new { Summary = "..." });
        config.UpdateScript = "this.Processed = true;"; // marker: the model round-trip completed and the update ran
        config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext(this);" }; // full document -> object/array top-level context

        store.Maintenance.Send(new AddGenAiOperation(config));
        var etlDone = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenSession())
        {
            session.Store(new RealProviderProduct
            {
                Title = "TechNova Phone X",
                Brand = new RealProviderBrand { Name = "TechNova", Category = "Consumer Electronics" }, // object-valued top-level context (the bug scenario)
                Tags = ["flagship", "5g"]
            }, "products/1");
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(6)));

        using (var session = store.OpenSession())
        {
            var product = session.Load<RealProviderProduct>("products/1");
            Assert.NotNull(product);
            // the object/nested context was accepted and the conversation completed without a wrapper/serialization error
            Assert.True(product.Processed);
        }
    }

    private class PersistedConversation { }

    private class RealProviderProduct
    {
        public string Title { get; set; }
        public RealProviderBrand Brand { get; set; }
        public string[] Tags { get; set; }
        public bool Processed { get; set; }
    }

    private class RealProviderBrand
    {
        public string Name { get; set; }
        public string Category { get; set; }
    }

    private sealed class Order
    {
        public string CompanyId { get; set; }
        public string Note { get; set; }
    }

    private sealed class Product
    {
        public string Brand { get; set; }
    }

    // Runs one GenAI conversation per context through RunTest(SendToModel) with a single query tool "Q". The mock
    // asks the model to call "Q" on the first turn of each conversation (detected by the absence of a tool message),
    // then answers once the query result comes back. Returns any thrown error plus the captured tool-result messages.
    private async Task<(Exception error, List<string> toolResults)> RunGenAiQueryToolAsync(
        List<DynamicJsonValue> contexts, string queryRql, string modelToolArgs, Action<IDocumentSession> seed)
    {
        using var store = GetDocumentStore();
        if (seed != null)
        {
            using var seedSession = store.OpenSession();
            seed(seedSession);
            seedSession.SaveChanges();
        }

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var config = new GenAiConfiguration
            {
                Name = "genai-26767-q",
                Identifier = "genai-26767-q",
                ConnectionStringName = "genai-cs",
                Collection = "Docs",
                Prompt = "Use the query tool to answer.",
                SampleObject = "{\"Answer\":\"a\"}",
                TestMode = true,
                MaxConcurrency = 1,
                GenAiTransformation = new GenAiTransformation { Script = "ai.genContext(this);" },
                Queries = [new AiAgentToolQuery("Q", "run the query", queryRql) { ParametersSampleObject = "{}" }]
            };

            using var task = new GenAiTask(config.Transforms[0], config, database, database.ServerStore);

            var toolResults = new List<string>();
            using var mock = new MockLlm(
                database.DocumentsStorage.ContextPool,
                new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")),
                onRequest: payload =>
                {
                    var messages = payload["messages"];
                    var toolMessages = messages?.Where(m => m["role"]?.ToString() == "tool").ToList() ?? new List<JToken>();
                    if (toolMessages.Count > 0)
                    {
                        foreach (var m in toolMessages)
                            toolResults.Add(m["content"]?.ToString() ?? string.Empty);
                        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(MockLlm.CreateAnswerResponse("\"done\"")) };
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(MockLlm.CreateToolCallResponse("Q", modelToolArgs)) };
                },
                conventions: ChatCompletionClient.ConventionsToUse);
            InjectMock(task, mock);

            var input = contexts.Select((ctx, i) => new GenAiResultItem
            {
                DocumentId = $"docs/{i + 1}",
                ContextOutput = new ContextOutput { Context = context.ReadObject(ctx, $"docs/{i + 1}"), IsCached = false, AiHash = $"docs/{i + 1}", Attachments = null }
            }).ToList();

            Exception error = null;
            try
            {
                task.RunTest(new TestGenAiScript { Configuration = config, TestStage = TestStage.SendToModel, Input = input }, context);
            }
            catch (Exception e)
            {
                error = e;
            }

            return (error, toolResults);
        }
    }
}
