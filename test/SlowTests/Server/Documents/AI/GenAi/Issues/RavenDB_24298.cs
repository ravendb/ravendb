using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.AI;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24298(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ConfigurationUpdateShouldTakeAffect(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore();
        const string docId = "posts/1";

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var sampleObject = JsonConvert.SerializeObject(new { Translation = "translated text" });
        var schema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObject);

        config.Prompt = "Translate this text to Polish";
        config.JsonSchema = schema;
        config.UpdateScript = "this.TextInPolish = $output.Translation;";
        config.Collection = "Posts";
        config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Text: this.Body });" };
        config.Identifier = "posts-translation-check";

        store.Maintenance.Send(new AddGenAiOperation(config));

        var etlDone = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("RavenDB is amazing", "Alex")], 
                "Understanding RavenDB Indexing", 
                "Indexes in RavenDB are powerful..."), 
                docId);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        var taskId = taskInfo.TaskId;

        // update the configuration
        config.UpdateScript = "this.Translated = $output.Translation;";
        var result = store.Maintenance.Send(new UpdateEtlOperation<AiConnectionString>(taskId, config));
        await Server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex, TimeSpan.FromSeconds(15));

        using (var session = store.OpenSession())
        {
            var post = session.Load<GenAiBasics.Post>(docId);
            post.Comments.Add(new GenAiBasics.Comment("spam", "evil bot"));
            session.SaveChanges();
        }

        var value = await WaitForValueAsync(async () =>
        {
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
                doc.TryGet("Translated", out string translation);
                return translation != null;
            }
        }, expectedVal: true, timeout: 60_000);


        Assert.True(value, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));


    }
}
