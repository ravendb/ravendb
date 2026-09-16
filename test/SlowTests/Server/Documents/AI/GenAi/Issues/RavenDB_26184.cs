using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    // RavenDB-26184 / RavenDB-26411 - GenAI ETL failure handling.
    //
    // Deterministic failures (too-many-tokens, refusal) write @gen-ai-hashes and never retry. Non-deterministic
    // failures are decided at the batch level: a partial batch parks the failed items via @refresh (no hash, no throw,
    // successes commit); a whole attempted batch that failed is thrown before any @refresh is stamped, letting the ETL
    // fallback/backoff handle it. Cached/already-hashed items never make a whole-attempted-batch failure look partial.
    public class RavenDB_26184(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string Marker = "ZZZMARKER";

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        public void IsWholeAttemptedBatchFailure_CountsOnlyAttemptedItems()
        {
            Assert.False(GenAiTask.IsWholeAttemptedBatchFailure([Sent(failed: false), Sent(failed: true)])); // partial
            Assert.True(GenAiTask.IsWholeAttemptedBatchFailure([Sent(failed: true), Sent(failed: true)]));    // whole attempted batch
            Assert.True(GenAiTask.IsWholeAttemptedBatchFailure([Cached(), Sent(failed: true)]));              // cached excluded -> whole
            Assert.False(GenAiTask.IsWholeAttemptedBatchFailure([Sent(failed: false), Sent(failed: false)])); // whole batch handled (e.g. deterministic) -> no throw
            Assert.False(GenAiTask.IsWholeAttemptedBatchFailure([Sent(failed: false)]));                      // success only
            Assert.False(GenAiTask.IsWholeAttemptedBatchFailure([Cached()]));                                 // nothing attempted
            Assert.False(GenAiTask.IsWholeAttemptedBatchFailure([]));
            return;

            static GenAiResultItem Sent(bool failed) => new() { UpdateHash = failed == false, ContextOutput = new ContextOutput { IsCached = false } };
            static GenAiResultItem Cached() => new() { UpdateHash = true, ContextOutput = new ContextOutput { IsCached = true } };
        }

        // Partial batch: one document succeeds, one fails non-deterministically (times out). The success is hashed, the
        // failure is parked via @refresh (no hash), and the batch commits (no throw).
        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task PartialBatch_HashesSuccess_ParksFailure_NoThrow(Options options, GenAiConfiguration config)
        {
            const int perCallDelayMs = 6_000;
            const int timeoutInSec = 3;

            options.ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Ai.GenAiSendToModelTimeout)] = timeoutInSec.ToString();

            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            ConfigureGenAi(config, "partial-batch");
            config.MaxConcurrency = 2; // keep both documents in one batch
            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlProcess = await WaitForGenAiProcessAsync(store);
            etlProcess.GetChatCompletionClient().ForTestingPurposesOnly().SimulateFailureAsync = async msg =>
            {
                if (msg.Contains(Marker) && msg.Contains("AI Agent Parameters:") == false)
                    await Task.Delay(perCallDelayMs); // only the marked document times out
            };

            const string okDoc = "posts/1";
            const string failDoc = "posts/2";
            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("a normal comment", "author") { Id = "1" }], "title", "body"), okDoc);
                session.Store(new GenAiBasics.Post([new GenAiBasics.Comment($"{Marker} times out", "author") { Id = "1" }], "title", "body"), failDoc);
                session.SaveChanges();
            }

            Assert.True(await WaitForValueAsync(() => HasSuccessHashAsync(store, okDoc, config.Identifier), true, timeout: 30_000),
                "the successful document should be hashed");
            Assert.False(await HasRefreshAsync(store, okDoc), "a successful document must not be parked via @refresh");
            Assert.True(await WaitForRefreshAsync(store, failDoc), "the non-deterministic failure should be parked via @refresh");
            Assert.False(await HasSuccessHashAsync(store, failDoc, config.Identifier), "a parked failure must not be hashed");
        }

        // Whole attempted batch fails non-deterministically (a single document that times out): it is thrown (ETL
        // fallback) and NOT parked - no @refresh is stamped, no hash. Once the model recovers, it is hashed.
        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WholeBatchFailure_Throws_DoesNotPark_ThenRecovers(Options options, GenAiConfiguration config)
        {
            const int perCallDelayMs = 6_000;
            const int timeoutInSec = 3;

            options.ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Ai.GenAiSendToModelTimeout)] = timeoutInSec.ToString();

            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            ConfigureGenAi(config, "whole-batch");
            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlProcess = await WaitForGenAiProcessAsync(store);
            var failing = new StrongBox<bool>(true);
            var modelCalls = 0;
            etlProcess.GetChatCompletionClient().ForTestingPurposesOnly().SimulateFailureAsync = async msg =>
            {
                if (msg.Contains(Marker) && msg.Contains("AI Agent Parameters:") == false)
                {
                    Interlocked.Increment(ref modelCalls);
                    if (Volatile.Read(ref failing.Value))
                        await Task.Delay(perCallDelayMs);
                }
            };

            const string docId = "posts/1";
            StoreMarkedPost(store, docId, "times out");

            Assert.True(await WaitForValueAsync(() => Task.FromResult(Volatile.Read(ref modelCalls) >= 1), true, timeout: 30_000),
                "the document should be attempted");

            // a whole attempted batch failure throws before stamping @refresh, so the document is neither parked nor hashed
            Assert.False(await HasRefreshAsync(store, docId), "a whole-attempted-batch non-deterministic failure must not stamp @refresh");
            Assert.False(await HasSuccessHashAsync(store, docId, config.Identifier), "a failed document must not be hashed");

            Volatile.Write(ref failing.Value, false);
            Assert.True(await WaitForValueAsync(() => HasSuccessHashAsync(store, docId, config.Identifier), true, timeout: 60_000),
                "after the model recovers, the document should be hashed");
        }

        private static void StoreMarkedPost(Raven.Client.Documents.IDocumentStore store, string docId, string comment)
        {
            using var session = store.OpenSession();
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment($"{Marker} {comment}", "author") { Id = "1" }], "title", "body"), docId);
            session.SaveChanges();
        }

        private static Task<bool> WaitForRefreshAsync(Raven.Client.Documents.IDocumentStore store, string docId)
            => WaitForValueAsync(() => HasRefreshAsync(store, docId), true, timeout: 30_000);

        private static async Task<bool> HasRefreshAsync(Raven.Client.Documents.IDocumentStore store, string docId)
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            return doc != null &&
                   doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                   metadata.TryGet(Constants.Documents.Metadata.Refresh, out object _);
        }

        private static void ConfigureGenAi(GenAiConfiguration config, string identifier)
        {
            config.Prompt = "Reply with the given text unchanged.";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Result = "text" });
            config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].Result = $output.Result;";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "for (const comment of this.Comments) ai.genContext({Text: comment.Text, Id: comment.Id});"
            };
            config.MaxConcurrency = 1;
            config.Identifier = identifier;
        }

        private async Task<GenAiTask> WaitForGenAiProcessAsync(Raven.Client.Documents.IDocumentStore store)
        {
            var db = await GetDatabase(store.Database);
            GenAiTask etlProcess = null;
            Assert.True(await WaitForValueAsync(() =>
            {
                etlProcess = db.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
                return Task.FromResult(etlProcess != null);
            }, true, timeout: 15_000), "GenAi ETL process was not loaded in time");
            return etlProcess;
        }

        private static async Task<bool> HasSuccessHashAsync(Raven.Client.Documents.IDocumentStore store, string docId, string identifier)
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            return doc != null &&
                   doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                   metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes) &&
                   hashes.TryGet(identifier, out BlittableJsonReaderArray arr) && arr.Length > 0;
        }
    }
}
