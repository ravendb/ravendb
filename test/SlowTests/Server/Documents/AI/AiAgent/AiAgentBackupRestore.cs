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
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentBackupRestore : ReplicationTestBase
{
    public AiAgentBackupRestore(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { BackupType.Backup })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { BackupType.Snapshot })]
    public async Task CanBackupAndRestoreAiAgents(Options options, GenAiConfiguration aiConfig, BackupType backupType)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = GetDatabaseName() + "_Restore" }.Initialize();

        using (var source = GetDocumentStore())
        {
            await source.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(aiConfig.Connection));

            var agents = GetAgents(aiConfig);
            foreach (var (agentName, agentConfig) in agents)
            {
                await source.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<AiAgentBasics.OutputSchema>(agentName, agentConfig));
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
                var destAgents = destRecord.AiAgents;
                Assert.NotNull(destAgents);
                Assert.Equal(2, destAgents.Count);
                var names = destAgents.Keys.ToArray();
                var destConfigs = destAgents.Values.ToArray();
                Assert.Equal("shopping assistant", names[0]);
                Assert.Equal("warehouse manager", names[1]);
                Assert.NotNull(destConfigs[0]);
                Assert.NotNull(destConfigs[1]);
                var configs = agents.Values.ToList();
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var converter = DocumentConventions.Default.Serialization.DefaultConverter;
                    var c0 = converter.ToBlittable(configs[0], context);
                    var d0 = converter.ToBlittable(destConfigs[0], context);
                    Assert.True(c0.Equals(d0));
                    var c1 = converter.ToBlittable(configs[1], context);
                    var d1 = converter.ToBlittable(destConfigs[1], context);
                    Assert.True(c1.Equals(d1));
                }
            }
        }
    }

    private static Dictionary<string, AiAgentConfiguration> GetAgents(GenAiConfiguration aiConfig)
    {
        var agent0 = new AiAgentConfiguration(aiConfig.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent0.Persistence = new AiAgentConfiguration.PersistenceConfiguration { Collection = "Chats", Expires = TimeSpan.FromDays(30) };

        agent0.Queries =
        [
            new AiAgentConfiguration.ToolQuery
            {
                Name = "ProductSearch",
                Description = "semantic search the store product catalog",
                Query = "from Products where vector.search(embedding.text(Name), $query)",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentConfiguration.ToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSampleObject = "{}"
            }
        ];


        var agent1 = new AiAgentConfiguration(aiConfig.ConnectionStringName,
            "You are an AI agent managing a warehouse.");

        agent1.Persistence = new AiAgentConfiguration.PersistenceConfiguration { Collection = "Chats", Expires = TimeSpan.FromDays(30) };

        agent1.Actions =
        [
            new AiAgentConfiguration.ToolAction
            {
                Name = "ProductSearch",
                Description = "semantic search the store product catalog",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentConfiguration.ToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
        ];

        return new Dictionary<string, AiAgentConfiguration>() { { "shopping assistant", agent0 }, { "warehouse manager", agent1 } };
    }
}
