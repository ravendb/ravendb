using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentBackupRestore : ReplicationTestBase
{
    public AiAgentBackupRestore(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { BackupType.Backup })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { BackupType.Snapshot })]
    public async Task CanBackupAndRestoreAiAgents(Options options, GenAiConfiguration aiConfig, BackupType backupType)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = GetDatabaseName() + "_Restore" }.Initialize();

        using (var source = GetDocumentStore())
        {
            await source.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(aiConfig.Connection));

            var agents = GetAgents(aiConfig);
            foreach (var agentConfig in agents)
            {
                await source.AI.CreateAgentAsync(agentConfig, AiAgentBasics.OutputSchema.Instance);
            }

            var backupOperation = await source.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = backupType,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));
            var result = (BackupResult)await backupOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            using var dis = Backup.RestoreDatabase(destination,
                new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(), 
                    DatabaseName = destination.Database
                });
            {
                var destRecord = await destination.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(destination.Database));
                var destConfigs = destRecord.AiAgents;
                Assert.NotNull(destConfigs);
                Assert.Equal(2, destConfigs.Count);
                Assert.NotNull(destConfigs[0]);
                Assert.NotNull(destConfigs[1]);
                Assert.Equal("shopping-assistant", destConfigs[0].Identifier);
                Assert.Equal("warehouse-manager", destConfigs[1].Identifier);
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var converter = DocumentConventions.Default.Serialization.DefaultConverter;
                    var c0 = converter.ToBlittable(agents[0], context);
                    var d0 = converter.ToBlittable(destConfigs[0], context);
                    Assert.Equal(c0, d0);
                    var c1 = converter.ToBlittable(agents[1], context);
                    var d1 = converter.ToBlittable(destConfigs[1], context);
                    Assert.Equal(c1, d1);
                }
            }
        }
    }

    private static List<AiAgentConfiguration> GetAgents(GenAiConfiguration aiConfig)
    {
        var agent0 = new AiAgentConfiguration("shopping assistant", aiConfig.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
        agent0.Identifier = "shopping-assistant";
        agent0.Parameters.Add(new AiAgentParameter("company"));
        agent0.ChatTrimming = null;
        agent0.Queries =
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


        var agent1 = new AiAgentConfiguration("warehouse manager", aiConfig.ConnectionStringName, "You are an AI agent managing a warehouse.");
        agent1.Identifier = "warehouse-manager";
        agent1.Actions =
        [
            new AiAgentToolAction
            {
                Name = "ProductSearch",
                Description = "semantic search the store product catalog",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentToolAction
            {
                Name = "RecentOrder", 
                Description = "Get the recent orders of the current user", 
                ParametersSampleObject = "{}"
            }
        ];
        agent1.ChatTrimming = null;
        return new List<AiAgentConfiguration>() { agent0, agent1 };
    }
}
