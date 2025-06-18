using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations;
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanBackupAndRestoreAiAgents(Options options, GenAiConfiguration aiConfig)
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

            var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
            var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

            var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
            await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            using var dis = Backup.RestoreDatabase(destination,
                new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = destination.Database });
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
                Assert.True(AiAgentConfigurationsAreEqualByValue(configs[0], destConfigs[0]));
                Assert.True(AiAgentConfigurationsAreEqualByValue(configs[1], destConfigs[1]));
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
                ParametersSchema = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentConfiguration.ToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSchema = "{}"
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
                ParametersSchema = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            },
            new AiAgentConfiguration.ToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSchema = "{}" }
        ];

        return new Dictionary<string, AiAgentConfiguration>() { { "shopping assistant", agent0 }, { "warehouse manager", agent1 } };
    }

    private static bool AiAgentConfigurationsAreEqualByValue(AiAgentConfiguration x, AiAgentConfiguration y)
    {
        if (ReferenceEquals(x, y))
            return true;
        if (x is null || y is null)
            return false;

        // compare simple properties
        if (string.Equals(x.ConnectionStringName, y.ConnectionStringName, StringComparison.Ordinal) == false ||
            string.Equals(x.SystemPrompt, y.SystemPrompt, StringComparison.Ordinal) == false ||
            string.Equals(x.OutputSchema, y.OutputSchema, StringComparison.Ordinal) == false)
        {
            return false;
        }

        // compare PersistenceConfiguration
        if (PersistenceEquals(x.Persistence, y.Persistence) == false)
            return false;

        // compare queries
        if (ListEquals(x.Queries, y.Queries, (a, b) =>
                ReferenceEquals(a, b) || (a != null && b != null && string.Equals(a.Name, b.Name, StringComparison.Ordinal) &&
                                          string.Equals(a.Description, b.Description, StringComparison.Ordinal) &&
                                          string.Equals(a.Query, b.Query, StringComparison.Ordinal) &&
                                          string.Equals(a.ParametersSchema, b.ParametersSchema, StringComparison.Ordinal))) == false)
            return false;

        // compare actions
        if (ListEquals(x.Actions, y.Actions, (a, b) =>
                ReferenceEquals(a, b) || (a != null && b != null && string.Equals(a.Name, b.Name, StringComparison.Ordinal) &&
                                          string.Equals(a.Description, b.Description, StringComparison.Ordinal) &&
                                          string.Equals(a.ParametersSchema, b.ParametersSchema, StringComparison.Ordinal))) == false)
            return false;

        return true;
    }

    private static bool PersistenceEquals(AiAgentConfiguration.PersistenceConfiguration a, AiAgentConfiguration.PersistenceConfiguration b)
    {
        if (ReferenceEquals(a, b))
            return true;
        if (a is null || b is null)
            return false;

        return string.Equals(a.Collection, b.Collection, StringComparison.Ordinal) && Nullable.Equals(a.Expires, b.Expires);
    }

    private static bool ListEquals<T>(List<T> a, List<T> b, Func<T, T, bool> elementComparer)
    {
        if (ReferenceEquals(a, b))
            return true;
        if (a is null || b is null || a.Count != b.Count)
            return false;

        for (int i = 0; i < a.Count; i++)
        {
            if (elementComparer(a[i], b[i]) == false)
                return false;
        }

        return true;
    }
}
