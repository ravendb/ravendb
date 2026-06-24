using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_26411(ITestOutputHelper output) : RavenTestBase(output)
    {
        // The GenAi 'SendToModel' timeout must apply per request (per document), not once across the whole batch.
        // Each model call is delayed so a single call fits its own window but two sequential calls do not fit one
        // shared batch window; with a per-batch deadline the 2nd document is starved and cancelled, with a
        // per-request deadline both succeed.
        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task GenAi_SendToModelTimeout_ShouldBeAppliedPerRequest_NotPerBatch(Options options, GenAiConfiguration config)
        {
            const int perCallDelayMs = 8_000;
            const int timeoutInSec = 15;
            const string marker = "ZZZMARKER";

            // The invariant that makes this test meaningful: a single call fits its own window (delay < timeout),
            // but two sequential calls would NOT fit a single shared batch window (timeout < 2*delay). Asserted so a
            // future tweak can't silently stop distinguishing a per-request timeout from a per-batch one.
            Assert.True(perCallDelayMs < timeoutInSec * 1000 && timeoutInSec * 1000 < 2 * perCallDelayMs,
                "test invariant: perCallDelay < timeout < 2*perCallDelay");

            options.ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Ai.GenAiSendToModelTimeout)] = timeoutInSec.ToString();

            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Reply with the given text unchanged.";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Result = "text" });
            config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].Result = $output.Result;";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "for (const comment of this.Comments) ai.genContext({Text: comment.Text, Id: comment.Id});"
            };
            config.MaxConcurrency = 1; // sequential, so the per-request vs per-batch difference is observable
            config.Identifier = "per-request-timeout";

            store.Maintenance.Send(new AddGenAiOperation(config));

            // EtlLoader populates the process asynchronously after AddGenAiOperation, so wait for it.
            var db = await GetDatabase(store.Database);
            GenAiTask etlProcess = null;
            Assert.True(await WaitForValueAsync(() =>
            {
                etlProcess = db.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
                return Task.FromResult(etlProcess != null);
            }, true, timeout: 15_000), "GenAi ETL process was not loaded in time");

            // Delay each model call once. Only the user-content message carries the marker; the "AI Agent Parameters:"
            // message also interpolates it, so we skip that. Assumes the task has no Queries/tools (a tool iteration
            // would re-serialize the prompt and delay twice).
            etlProcess.GetChatCompletionClient().ForTestingPurposesOnly().SimulateFailureAsync = async msg =>
            {
                if (msg.Contains(marker) && msg.Contains("AI Agent Parameters:") == false)
                    await Task.Delay(perCallDelayMs);
            };

            const string docId = "posts/1";
            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([
                    new GenAiBasics.Comment($"{marker} first comment", "author") { Id = "1" },
                    new GenAiBasics.Comment($"{marker} second comment", "author") { Id = "2" }
                ], "title", "body"), docId);
                session.SaveChanges();
            }

            // both contexts must be processed - the 2nd one must not be cancelled by a shared batch timeout
            var processed = await WaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
                if (doc == null || doc.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments) == false)
                    return 0;

                return comments.Cast<BlittableJsonReaderObject>().Count(c => c.TryGet("Result", out string _));
            }, 2, timeout: 90_000);

            Assert.Equal(2, processed);

            // and there must be no load errors: a per-batch timeout would have logged a cancellation for the 2nd context
            var errors = (await Etl.GetItemLoadErrorsAsync(store.Database, config)).ToList();
            Assert.True(errors.Count == 0, $"Expected no load errors, but got: {string.Join(" | ", errors.Select(e => e.Error))}");
        }
    }
}
