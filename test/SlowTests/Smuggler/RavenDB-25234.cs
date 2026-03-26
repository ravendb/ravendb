using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using SlowTests.Server.Documents.AI.AiAgent;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Smuggler;

public class RavenDB_25234 : RavenTestBase
{
    public RavenDB_25234(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanSkipAiItemsOnImport(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        // add AI connection string
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        // add a sample GenAI ETL
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        // add a sample AI Agent
        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.")
            {
                Identifier = "shopping-assistant"
            };
        agent.Parameters.Add(new AiAgentParameter("company"));
        agent.ChatTrimming = null;
        agent.Queries =
        [
            new AiAgentToolQuery
            {
                Name = "ProductSearch",
                Description = "semantic search the store product catalog",
                Query = "from Products where vector.search(embedding.text(Name), $query)",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSampleObject = "{}"
            }
        ];

        await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance);

        var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

        Assert.NotEmpty(record.AiConnectionStrings);
        Assert.NotEmpty(record.GenAis);
        Assert.NotEmpty(record.AiAgents);

        var file = GetTempFileName();

        var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        // import into a new database, skipping AI items

        using var store2 = GetDocumentStore(options);

        var importOptions = new DatabaseSmugglerImportOptions();
        importOptions.OperateOnDatabaseRecordTypes &= ~DatabaseRecordItemType.AiConnectionStrings;
        importOptions.OperateOnDatabaseRecordTypes &= ~DatabaseRecordItemType.GenAiEtls;
        importOptions.OperateOnDatabaseRecordTypes &= ~DatabaseRecordItemType.AiAgents;

        operation = await store2.Smuggler.ImportAsync(importOptions, file);
        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        record = store2.Maintenance.Server.Send(new GetDatabaseRecordOperation(store2.Database));

        Assert.Empty(record.AiConnectionStrings);
        Assert.Empty(record.GenAis);
        Assert.Empty(record.AiAgents);
    }
}
