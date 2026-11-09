using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using PatchDocumentCommand = Raven.Server.Documents.Patch.PatchDocumentCommand;
using PatchRequestType = Raven.Server.Documents.Patch.PatchRequestType;
using SimpleAssignmentPatch = Raven.Server.Documents.Patch.SimpleAssignmentPatch;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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
            yield return ["this.Note = args.z; this.Extra = args.e; this.Age = args.a; this.Name = args.n;", // not in the order of the document
                new Dictionary<string, object> { ["z"] = "note", ["e"] = "new", ["a"] = 7L, ["n"] = "x" }];
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
            yield return ["this.Active = false;", new Dictionary<string, object>()]; // boolean literal
            yield return ["this.Active = true;", new Dictionary<string, object>()]; // not modified
            yield return ["this.Note = null; this.Extra = null;", new Dictionary<string, object>()]; // null literal, existing and new field
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
                        Assert.True(SimpleAssignmentPatch.TryGet(patch).TryApply(context, document.Data, preparedArgs, out prepared));
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

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PreparedPatchOfADocumentDeletedBeforeTheMerger(bool recreated)
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
                        Assert.True(SimpleAssignmentPatch.TryGet(patch).TryApply(context, document.Data, preparedArgs, out prepared));
                    }

                    // the document is deleted (and maybe created again) after the snapshot was taken
                    using (var session = store.OpenSession())
                    {
                        session.Delete("items/1");
                        session.SaveChanges();
                    }
                    if (recreated)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new Item { Name = "recreated", Age = 3 }, "items/1");
                            session.SaveChanges();
                        }
                    }

                    var args = context.ReadObject(new DynamicJsonValue { ["v"] = "from-script" }, "args");
                    using (var command = new PatchDocumentCommand(context, "items/1", null, false, (patch, args), (null, null), null,
                               database.IdentityPartsSeparator, isTest: false, debugMode: false, collectResultsNeeded: true, returnDocument: false))
                    {
                        command.SetPrepared("items/1", changeVector, flags, prepared);
                        await database.TxMerger.Enqueue(command);
                        Assert.Equal(recreated ? PatchStatus.Patched : PatchStatus.DocumentDoesNotExist, command.PatchResult.Status);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    if (recreated == false)
                    {
                        Assert.Null(item); // the snapshot is not written over a delete
                        return;
                    }
                    Assert.Equal("from-script", item.Name); // the script ran on the new document
                    Assert.Equal(3, item.Age);
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

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Sharding)]
        public async Task AShardRefusesAPatchOfADocumentItDoesNotOwn()
        {
            using var store = Sharding.GetDocumentStore();
            const string id = "items/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Name = "original" }, id);
                await session.SaveChangesAsync();
            }

            var owner = await Sharding.GetShardNumberForAsync(store, id);
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var other = record.Sharding.Shards.Keys.First(shard => shard != owner);
            var wrongShard = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(ShardHelper.ToShardName(store.Database, other));

            AssertRefused("changed"); // no copy on the shard: patched in the merger

            // a copy on the wrong shard, as a bucket migration leaves before its cleanup
            wrongShard.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;
            using (var session = store.OpenAsyncSession(wrongShard.Name))
            {
                await session.StoreAsync(new Item { Name = "original" }, id);
                await session.SaveChangesAsync();
            }
            wrongShard.ForTestingPurposesOnly().EnableWritesToTheWrongShard = false;

            AssertRefused("changed"); // prepared outside the merger, written in it
            AssertRefused("original"); // not modified: answered outside the merger

            void AssertRefused(string value)
            {
                foreach (var script in new[] { "this.Name = args.v;", "this.Name = args.v; void 0;" }) // the fast path, the script engine
                {
                    var request = new PatchRequest { Script = script, Values = { ["v"] = value } };
                    Assert.Throws<ShardMismatchException>(() => store.Operations.ForDatabase(wrongShard.Name).Send(new PatchOperation(id, null, request)));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("this.Name = args.v; this.Age += 1;", true)]
        [InlineData("this[args.f] = args.v;", true)]
        [InlineData("this.Name = args.v; this.Name += args.w;", false)] // a field updated twice
        [InlineData("this.Name = args.v; this['Name'] = args.w;", false)]
        [InlineData("this[args.f] = args.v; this.Age += 1;", false)] // the field from the arguments may be Age
        [InlineData("this.F0 = 0; this.F1 = 1; this.F2 = 2; this.F3 = 3; this.F4 = 4; this.F5 = 5; this.F6 = 6; this.F7 = 7; this.F8 = 8; this.F9 = 9; this.F10 = 10; this.F11 = 11; this.F12 = 12; this.F13 = 13; this.F14 = 14; this.F15 = 15;", true)]
        [InlineData("this.F0 = 0; this.F1 = 1; this.F2 = 2; this.F3 = 3; this.F4 = 4; this.F5 = 5; this.F6 = 6; this.F7 = 7; this.F8 = 8; this.F9 = 9; this.F10 = 10; this.F11 = 11; this.F12 = 12; this.F13 = 13; this.F14 = 14; this.F15 = 15; this.F16 = 16;", false)] // more updates than the fast path holds
        [InlineData("this.Active = true; this.Done = false; this.Note = null;", true)]
        public void ScriptsThatTakeTheFastPath(string script, bool simple)
        {
            Assert.Equal(simple, SimpleAssignmentPatch.TryGet(new Raven.Server.Documents.Patch.PatchRequest(script, PatchRequestType.Patch)) != null);
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData(false)]
        [InlineData(true)] // repeated text: the stored string is compressed
        public void LongStringsAreComparedAndWrittenWithoutAManagedCopy(bool repeated)
        {
            const int length = 100 * 1024;
            var random = new byte[length / 4 * 3];
            new Random(42).NextBytes(random);
            var text = repeated ? new string('a', length) : Convert.ToBase64String(random); // random text does not compress
            var other = text[..^1] + "!"; // same length, last character differs

            var argsMode = repeated ? BlittableJsonDocumentBuilder.UsageMode.ToDisk : BlittableJsonDocumentBuilder.UsageMode.None; // compressed arguments too
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var document = context.ReadObject(new DynamicJsonValue { ["Text"] = text }, "items/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            using (var same = context.ReadObject(new DynamicJsonValue { ["v"] = text }, "args", argsMode))
            using (var changed = context.ReadObject(new DynamicJsonValue { ["v"] = other }, "args", argsMode))
            {
                document.TryGetMember("Text", out object stored);
                Assert.Equal(repeated, stored is LazyCompressedStringValue);

                var simple = SimpleAssignmentPatch.TryGet(new Raven.Server.Documents.Patch.PatchRequest("this.Text = args.v;", PatchRequestType.Patch));
                Assert.True(simple.TryApply(context, document, same, out var modified)); // warm up
                Assert.Null(modified);

                var before = GC.GetAllocatedBytesForCurrentThread();
                Assert.True(simple.TryApply(context, document, same, out modified));
                var notModifiedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.Null(modified);

                before = GC.GetAllocatedBytesForCurrentThread();
                Assert.True(simple.TryApply(context, document, changed, out modified));
                var modifiedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
                using (modified)
                {
                    Assert.NotNull(modified);
                    Assert.True(modified.TryGet("Text", out string written));
                    Assert.Equal(other, written);
                }

                // a UTF-16 copy of the stored or the argument value alone would be 200 KB
                Output.WriteLine($"managed allocations: not modified {notModifiedBytes:#,#0} bytes, modified {modifiedBytes:#,#0} bytes");
                Assert.True(notModifiedBytes < 16 * 1024, $"{notModifiedBytes:#,#0} bytes");
                Assert.True(modifiedBytes < 16 * 1024, $"{modifiedBytes:#,#0} bytes");
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("this.Age++;", true)]
        [InlineData("this.Active = false; this.Note = null;", true)]
        [InlineData("this.Age += args.v;", false)] // reads an argument
        public void ARequestWithoutValuesTakesTheFastPathWhenTheScriptReadsNoArgument(string script, bool applied)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var document = context.ReadObject(new DynamicJsonValue { ["Age"] = 3L, ["Active"] = true, ["Note"] = "n" }, "items/1"))
            {
                var simple = SimpleAssignmentPatch.TryGet(new Raven.Server.Documents.Patch.PatchRequest(script, PatchRequestType.Patch));
                Assert.Equal(applied, simple.TryApply(context, document, args: null, out var modified));
                Assert.Equal(applied, modified != null);
                modified?.Dispose();
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("this.Name = foo(args.v);")]
        [InlineData("if (this.Age) this.Age = 1;")]
        [InlineData("this.Age = this.Age ? 1 : 2;")]
        [InlineData("this.Tags = {};")]
        public void ScriptsThatCannotBeSimpleAreNotParsedOrCached(string script)
        {
            script += $" // {Guid.NewGuid()}";
            Assert.Null(SimpleAssignmentPatch.TryGet(new Raven.Server.Documents.Patch.PatchRequest(script, PatchRequestType.Patch)));
            Assert.False(SimpleAssignmentPatch.IsCached(script));
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void AScriptInUseOutlivesNewerScripts()
        {
            var cache = new SimpleAssignmentPatch.ScriptCache(size: 4);
            cache.Add("hot", null);
            for (var i = 0; i < 100; i++)
            {
                Assert.True(cache.TryGet("hot", out _)); // used between the arrivals of new scripts
                cache.Add($"once/{i}", null);
                Assert.True(cache.Count <= 4);
            }
            Assert.True(cache.Contains("hot"));
            Assert.False(cache.Contains("once/0"));
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void ConcurrentMissesAddAScriptOnce()
        {
            var cache = new SimpleAssignmentPatch.ScriptCache(size: 4);
            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 8 }, _ => cache.Add("script", null));
            Assert.Equal(1, cache.Count);
            Assert.Equal(1, cache.QueuedForEviction);
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void LowMemoryClearsTheScriptCache()
        {
            var script = $"this.Name = args.v; // {Guid.NewGuid()}";
            Assert.NotNull(SimpleAssignmentPatch.TryGet(new Raven.Server.Documents.Patch.PatchRequest(script, PatchRequestType.Patch)));
            Assert.True(SimpleAssignmentPatch.IsCached(script));

            SimpleAssignmentPatch.LowMemoryForTesting();
            Assert.False(SimpleAssignmentPatch.IsCached(script));
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
                    return collection + " " + body; // in insertion order: a patch keeps the order of the properties
            }
        }
    }
}
