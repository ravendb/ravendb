using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class FilteredPullReplicationClusterConvergenceTests : FilteredPullReplicationClusterConvergenceTestBase
    {
        public FilteredPullReplicationClusterConvergenceTests(ITestOutputHelper output) : base(output)
        {
        }

        public static IEnumerable<object[]> ScenarioMatrix()
        {
            var scenarioIds = ScenarioCatalog.Definitions.Value.Keys.OrderBy(x => x).ToArray();
            foreach (var scenarioId in scenarioIds)
            {
                yield return [scenarioId, BridgeTicketMutationMode.None];
                yield return [scenarioId, BridgeTicketMutationMode.ModifyOnTarget];
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(ScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_All_Nodes(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [ScenarioId.AllThreeAllDifferentCWins, BridgeTicketMutationMode.None], Skip = "Filtered pull replication is not supported on sharded databases.")]
        public Task Should_Not_Run_On_Sharded_Databases(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);
    }
}
