using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25808 : RavenTestBase
    {
        public RavenDB_25808(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldResendWhenExistingTaskIsV1AndSampleObjectChangedBumpsToV2(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // ---- Task config (V1) ----
            var sampleObjectV1 = JsonConvert.SerializeObject(new { Translation = "translated text" });
            var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObjectV1);

            config.Prompt = "Translate this text to Polish";
            config.JsonSchema = schema;
            config.UpdateScript = "this.TextInPolish = $output.Translation;";
            config.Collection = "Posts";
            config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Text: this.Body });" };
            config.Identifier = "posts-translation-v1-to-v2";
            config.Version = null;
            config.SampleObject = sampleObjectV1;


            var command = new Raven.Server.ServerWide.Commands.AI.AddGenAiCommand(
                config,
                store.Database,
                changeVector: null,
                uniqueRequestId: Guid.NewGuid().ToString());

            await Server.ServerStore.SendToLeaderAsync(command);

            // ---- Run once (stores V1 hash) ----
            var etlDone = Etl.WaitForEtlToComplete(store);
            const string docId = "posts/1";

            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("c1", "a1")], "t", "Hello World"), docId);
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            string originalHash;
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Sparrow.Json.BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out Sparrow.Json.BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out Sparrow.Json.BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out Sparrow.Json.BlittableJsonReaderArray hashesArray));
                Assert.NotNull(hashesArray);

                originalHash = hashesArray.Last().ToString();
            }

            // get taskId + process
            var db = await GetDatabase(store.Database);
            var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
            Assert.NotNull(etlProcess);
            var taskId = etlProcess.TaskId;

            // disable task
            await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));
            await AssertWaitForTrueAsync(() => Task.FromResult(etlProcess.IsRunning == false));

            // ---- Change SampleObject -> should bump task to V2 inside AddGenAiCommand ----
            var sampleObjectV2 = JsonConvert.SerializeObject(new
            {
                Translation = "translated text",
                Extra = "new field to force SampleObject change"
            });

            config.SampleObject = sampleObjectV2;

            var baselineUtc = DateTime.UtcNow;
            //simulating older client cause the Version is configured to Null.
            await store.Maintenance.SendAsync(new UpdateGenAiOperation(taskId, config)); // task will be re-enabled

            // ---- DIAGNOSTIC ASSERTION ----
            // Verify the server actually bumped the version to WithSampleObject BEFORE waiting for ETL.
            var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
            var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
            Assert.NotNull(genAiTaskInfo);
            Assert.Equal(GenAiConfiguration.WithSampleObject, genAiTaskInfo.Configuration.Version);

            // ---- Touch doc without changing context (Body stays same) ----
            etlDone = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenSession())
            {
                var post = session.Load<GenAiBasics.Post>(docId);
                post.Comments.Add(new GenAiBasics.Comment("new comment", "bot")); // context is Body only -> unchanged
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            // Update process reference
            etlProcess = db.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
            Assert.NotNull(etlProcess);

            // ---- Assert it resent (hash mismatch due to including SampleObject) ----
            EtlPerformanceStats[] stats = null;
            var ok = await WaitForValueAsync(() =>
            {
                stats = etlProcess.GetPerformanceStats()
                    .Where(x => x != null && x.Started >= baselineUtc && x.NumberOfLoadedItems > 0)
                    .ToArray();

                return stats.Length > 0;
            }, expectedVal: true, timeout: 60_000);

            Assert.True(ok, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));

            var resent = stats.Any(s =>
            {
                var load = s.Details.Operations[^1];
                var op = load.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel)
                    as GenAiPerformanceOperation;

                return op is { NumberOfContextObjects: 1, TotalSentToModel: 1, TotalCachedContexts: 0 };
            });

            Assert.True(resent, "Expected the context to be resent after SampleObject change (V1 -> V2).");

            // ---- Hash should change and count should increase ----
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Sparrow.Json.BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out Sparrow.Json.BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out Sparrow.Json.BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out Sparrow.Json.BlittableJsonReaderArray hashesArray));

                var newHash = hashesArray.Last().ToString();
                Assert.NotEqual(originalHash, newHash);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldNotResend_WhenExistingTaskIsV1_AndSampleObjectIsUnchanged_StaysV1(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // ---- Task config (V1) ----
            var sampleObjectV1 = JsonConvert.SerializeObject(new { Translation = "translated text" });
            var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObjectV1);

            config.Prompt = "Translate this text to Polish";
            config.JsonSchema = schema;
            config.UpdateScript = "this.TextInPolish = $output.Translation;";
            config.Collection = "Posts";
            config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Text: this.Body });" };
            config.Identifier = "posts-translation-v1-stays-v1";
            config.Version = null;
            config.SampleObject = sampleObjectV1;

            var command = new Raven.Server.ServerWide.Commands.AI.AddGenAiCommand(
                config,
                store.Database,
                changeVector: null,
                uniqueRequestId: Guid.NewGuid().ToString());

            await Server.ServerStore.SendToLeaderAsync(command);

            // ---- Run once (stores V1 hash) ----
            var etlDone = Etl.WaitForEtlToComplete(store);
            const string docId = "posts/2";

            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("c1", "a1")], "t", "Hello World"), docId);
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            string originalHash;
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Sparrow.Json.BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out Sparrow.Json.BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out Sparrow.Json.BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out Sparrow.Json.BlittableJsonReaderArray hashesArray));

                originalHash = hashesArray.Last().ToString();
            }

            // get taskId + process
            var db = await GetDatabase(store.Database);
            var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
            Assert.NotNull(etlProcess);
            var taskId = etlProcess.TaskId;

            // disable task
            await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));
            await AssertWaitForTrueAsync(() => Task.FromResult(etlProcess.IsRunning == false));

            await store.Maintenance.SendAsync(new UpdateGenAiOperation(taskId, config));

            // ---- Touch doc without changing context (Body stays same) ----
            var baselineUtc = DateTime.UtcNow;
            etlDone = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenSession())
            {
                var post = session.Load<GenAiBasics.Post>(docId);
                post.Comments.Add(new GenAiBasics.Comment("new comment", "bot")); // context is Body only -> unchanged
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            // Update process reference
            etlProcess = db.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
            Assert.NotNull(etlProcess);

            // ---- Assert it was CACHED (did not resend, because hash algorithm remained V1) ----
            EtlPerformanceStats[] stats = null;
            var ok = await WaitForValueAsync(() =>
            {
                stats = etlProcess.GetPerformanceStats()
                    .Where(x => x != null && x.Started >= baselineUtc && x.NumberOfLoadedItems > 0)
                    .ToArray();

                return stats.Length > 0;
            }, expectedVal: true, timeout: 60_000);

            Assert.True(ok, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));

            var usedCache = stats.Any(s =>
            {
                var load = s.Details.Operations[^1];
                var op = load.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel)
                    as GenAiPerformanceOperation;

                return op is { NumberOfContextObjects: 1, TotalSentToModel: 0, TotalCachedContexts: 1 };
            });

            Assert.True(usedCache, "Expected the context to be cached since SampleObject was unchanged and task stayed V1.");

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Sparrow.Json.BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out Sparrow.Json.BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out Sparrow.Json.BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out Sparrow.Json.BlittableJsonReaderArray hashesArray));

                var newHash = hashesArray.Last().ToString();
                Assert.Equal(originalHash, newHash);

                var finalTaskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
                var finalGenAiTaskInfo = finalTaskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;

                Assert.NotNull(finalGenAiTaskInfo);
                Assert.Equal(null, finalGenAiTaskInfo.Configuration.Version);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldBumpIntoV2InCaseOfAddGenAiOperation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // ---- Initial task config (V1) ----
            var sampleObjectV1 = JsonConvert.SerializeObject(new
            {
                Translation = "translated text"
            });

            var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObjectV1);

            config.Prompt = "Translate this text to Polish";
            config.JsonSchema = schema;
            config.UpdateScript = "this.TextInPolish = $output.Translation;";
            config.Collection = "Posts";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "ai.genContext({ Text: this.Body });"
            };
            config.Identifier = "posts-translation-v1-to-v2";
            config.Version = null;

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));
            var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));

            var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
            Assert.NotNull(genAiTaskInfo);
            Assert.Equal(GenAiConfiguration.WithSampleObject, genAiTaskInfo.Configuration.Version);
        }
    }
}
