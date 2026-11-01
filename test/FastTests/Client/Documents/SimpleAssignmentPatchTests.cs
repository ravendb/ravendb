using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using PatchDocumentCommand = Raven.Server.Documents.Patch.PatchDocumentCommand;
using PatchRequestType = Raven.Server.Documents.Patch.PatchRequestType;
using SimpleAssignmentPatch = Raven.Server.Documents.Patch.SimpleAssignmentPatch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Documents
{
    public class SimpleAssignmentPatchTests : RavenTestBase
    {
        public SimpleAssignmentPatchTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Name { get; set; }
            public long Age { get; set; }
            public double Score { get; set; }
            public bool Active { get; set; }
            public string Note { get; set; }
            public long Big { get; set; }
            public Dictionary<string, string> Tags { get; set; }
        }

        // the trailing statement makes the script ineligible for the fast path, so it runs through the script engine
        private const string ForceScriptEngine = "; void 0;";

        public static IEnumerable<object[]> Cases()
        {
            yield return ["this.Name = args.v;", new Dictionary<string, object> { ["v"] = "changed" }];
            yield return ["this['Name'] = args.v;", new Dictionary<string, object> { ["v"] = "changed" }];
            yield return ["this[\"Name\"] = args.v;", new Dictionary<string, object> { ["v"] = "changed" }];
            yield return ["this[args.f] = args.v;", new Dictionary<string, object> { ["f"] = "Age", ["v"] = 42L }];
            yield return ["this.Name = args.n; this.Age = args.a; this.Score = args.s; this.Active = args.b; this.Note = args.z;",
                new Dictionary<string, object> { ["n"] = "x", ["a"] = 7L, ["s"] = 1.5, ["b"] = false, ["z"] = null }];
            yield return ["this.Name = args.v", new Dictionary<string, object> { ["v"] = "original" }]; // not modified
            yield return ["this.Name = args.a; this.Name = args.b;", new Dictionary<string, object> { ["a"] = "temp", ["b"] = "original" }]; // last wins, not modified
            yield return ["this.Extra = args.v;", new Dictionary<string, object> { ["v"] = "new field" }];
            yield return ["this[args.f] = args.v;", new Dictionary<string, object> { ["f"] = "Tags", ["v"] = new Dictionary<string, string> { ["k"] = "v" } }]; // object value
            yield return ["this[args.f] = args.v;", new Dictionary<string, object> { ["f"] = "@flags", ["v"] = "x" }]; // metadata-like field
            yield return ["this.Name = args.missing;", new Dictionary<string, object>()]; // undefined argument
            yield return ["this.Age++;", new Dictionary<string, object>()];
            yield return ["++this.Age;", new Dictionary<string, object>()];
            yield return ["this.Age--; this.Age--;", new Dictionary<string, object>()];
            yield return ["this.Age += args.v;", new Dictionary<string, object> { ["v"] = 5L }];
            yield return ["this.Age -= args.v;", new Dictionary<string, object> { ["v"] = 5L }];
            yield return ["this.Age += args.v;", new Dictionary<string, object> { ["v"] = 0.5 }]; // long field + double
            yield return ["this.Age += 1;", new Dictionary<string, object>()]; // literal
            yield return ["this.Age += 0;", new Dictionary<string, object>()]; // not modified
            yield return ["this.Score += args.v;", new Dictionary<string, object> { ["v"] = 1.5 }];
            yield return ["this.Score += args.v;", new Dictionary<string, object> { ["v"] = 0.75 }]; // double field, integral result
            yield return ["this.Score = args.v;", new Dictionary<string, object> { ["v"] = 42L }]; // integer into a double field
            yield return ["this.Count = args.v; this.Count += args.v;", new Dictionary<string, object> { ["v"] = 3L }]; // new field, then its own value
            yield return ["this.Age = this.Age + args.v;", new Dictionary<string, object> { ["v"] = 2L }];
            yield return ["this[args.f] = this[args.f] + args.v;", new Dictionary<string, object> { ["f"] = "Age", ["v"] = 2L }];
            yield return ["this['Age'] = this['Age'] - args.v;", new Dictionary<string, object> { ["v"] = 2L }];
            yield return ["this.Name += args.v;", new Dictionary<string, object> { ["v"] = "-suffix" }]; // string concatenation
            yield return ["this.Name += args.v;", new Dictionary<string, object> { ["v"] = 7L }]; // string + number
            yield return ["this.Name = this.Name + '!';", new Dictionary<string, object>()];
            yield return ["this.Big++;", new Dictionary<string, object>()]; // beyond 2^53: JavaScript rounds
            yield return ["this.Missing++;", new Dictionary<string, object>()]; // undefined
            yield return ["this.Note = args.v; this.Note++;", new Dictionary<string, object> { ["v"] = null }]; // null
            yield return ["this.Active += 1;", new Dictionary<string, object>()]; // bool
            yield return ["this.Age = this.Score + args.v;", new Dictionary<string, object> { ["v"] = 1L }]; // a different field
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [MemberData(nameof(Cases))]
        public void SimpleAssignmentsMatchTheScriptEngine(string script, Dictionary<string, object> values)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var id in new[] { "fast/1", "engine/1" })
                        session.Store(new Item { Name = "original", Age = 3, Score = 2.25, Active = true, Note = "n", Big = 9007199254740993, Tags = new Dictionary<string, string> { ["a"] = "b" } }, id);
                    session.SaveChanges();
                }

                var fastStatus = Patch(store, "fast/1", script, values);
                var engineStatus = Patch(store, "engine/1", script + ForceScriptEngine, values);

                Assert.Equal(engineStatus, fastStatus);
                Assert.Equal(Body(store, "engine/1"), Body(store, "fast/1"));
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("this.Name = args.v;", "changed", PatchStatus.Patched)]
        [InlineData("this.Name = args.v;", "original", PatchStatus.NotModified)]
        public void SimpleAssignmentReturnsTheModifiedDocument(string script, string value, PatchStatus expectedStatus)
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new Item { Name = "original" }, "items/1");
                session.SaveChanges();

                var request = new PatchRequest { Script = script, Values = { ["v"] = value } };
                var command = new PatchOperation.PatchCommand(store.Conventions, session.Advanced.Context, "items/1", null, request,
                    patchIfMissing: null, skipPatchIfChangeVectorMismatch: false, returnDebugInformation: false, test: false);
                session.Advanced.RequestExecutor.Execute(command, session.Advanced.Context);

                Assert.Equal(expectedStatus, command.Result.Status);
                Assert.NotNull(command.Result.ModifiedDocument);
                Assert.Equal(value, command.Result.ModifiedDocument["Name"].ToString());
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void SimpleAssignmentOnMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Equal(PatchStatus.DocumentDoesNotExist, Patch(store, "missing/1", "this.Name = args.v;", new Dictionary<string, object> { ["v"] = "x" }));
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void SimpleAssignmentChangesTheChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Name = "original" }, "items/1");
                    session.SaveChanges();
                }

                string before;
                using (var session = store.OpenSession())
                    before = session.Advanced.GetChangeVectorFor(session.Load<Item>("items/1"));

                Assert.Equal(PatchStatus.Patched, Patch(store, "items/1", "this.Name = args.v;", new Dictionary<string, object> { ["v"] = "changed" }));

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    Assert.Equal("changed", item.Name);
                    Assert.NotEqual(before, session.Advanced.GetChangeVectorFor(item));
                    Assert.Equal("Items", session.Advanced.GetMetadataFor(item)[Constants.Documents.Metadata.Collection]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task StalePreparedDocumentFallsBackToTheScript(bool simplePatchKnown)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Name = "original", Age = 1 }, "items/1");
                    session.SaveChanges();
                }

                var database = await GetDatabase(store.Database);
                var patch = new Raven.Server.Documents.Patch.PatchRequest("this.Name = args.v;", PatchRequestType.Patch);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    string changeVector;
                    DocumentFlags flags;
                    BlittableJsonReaderObject prepared;
                    using (context.OpenReadTransaction())
                    using (var document = database.DocumentsStorage.Get(context, "items/1"))
                    {
                        changeVector = document.ChangeVector;
                        flags = document.Flags;
                        var preparedArgs = context.ReadObject(new DynamicJsonValue { ["v"] = "from-snapshot" }, "args");
                        Assert.True(SimpleAssignmentPatch.TryGet(patch).TryApply(context, document.Id, document.Data, preparedArgs, out prepared));
                    }

                    // the document changes after the snapshot was taken
                    using (var session = store.OpenSession())
                    {
                        session.Load<Item>("items/1").Age = 2;
                        session.SaveChanges();
                    }

                    var args = context.ReadObject(new DynamicJsonValue { ["v"] = "from-script" }, "args");
                    using (var command = new PatchDocumentCommand(context, "items/1", null, false, (patch, args), (null, null), null,
                               database.IdentityPartsSeparator, isTest: false, debugMode: false, collectResultsNeeded: true, returnDocument: false))
                    {
                        command.SetPrepared("items/1", changeVector, flags, prepared);
                        if (simplePatchKnown)
                            command.SetSimplePatch(SimpleAssignmentPatch.TryGet(patch)); // re-applied without the script engine
                        await database.TxMerger.Enqueue(command);
                        Assert.Equal(PatchStatus.Patched, command.PatchResult.Status);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    Assert.Equal("from-script", item.Name); // the script ran on the current document...
                    Assert.Equal(2, item.Age);              // ...so the concurrent change survived
                }
            }
        }

        private static PatchStatus Patch(IDocumentStore store, string id, string script, Dictionary<string, object> values)
        {
            var request = new PatchRequest { Script = script };
            foreach (var (key, value) in values)
                request.Values[key] = value;
            return store.Operations.Send(new PatchOperation(id, null, request));
        }

        private static string Body(IDocumentStore store, string id)
        {
            using (var commands = store.Commands())
            {
                var document = commands.Get(id).BlittableJson;
                document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
                metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection);
                document.Modifications = new DynamicJsonValue(document);
                document.Modifications.Remove(Constants.Documents.Metadata.Key);
                using (var body = commands.Context.ReadObject(document, id))
                    return collection + " " + SortedJson(body); // the field order is not part of the result
            }
        }

        private static string SortedJson(BlittableJsonReaderObject document)
        {
            var node = System.Text.Json.Nodes.JsonNode.Parse(document.ToString());
            return Sort(node).ToJsonString();

            static System.Text.Json.Nodes.JsonNode Sort(System.Text.Json.Nodes.JsonNode n) => n switch
            {
                System.Text.Json.Nodes.JsonObject o => new System.Text.Json.Nodes.JsonObject(o.OrderBy(p => p.Key, StringComparer.Ordinal)
                    .Select(p => KeyValuePair.Create(p.Key, p.Value == null ? null : Sort(p.Value)))),
                System.Text.Json.Nodes.JsonArray a => new System.Text.Json.Nodes.JsonArray(a.Select(x => x == null ? null : Sort(x)).ToArray()),
                _ => System.Text.Json.Nodes.JsonNode.Parse(n.ToJsonString())
            };
        }
    }
}
