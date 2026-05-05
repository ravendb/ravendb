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

        private static IEnumerable<object[]> BuildScenarioMatrix(params ScenarioGroup[] groups)
        {
            var scenarioIds = ScenarioCatalog.Definitions.Value
                .Where(x => groups.Contains(x.Value.Group))
                .Select(x => x.Key)
                .OrderBy(x => x)
                .ToArray();

            foreach (var scenarioId in scenarioIds)
            {
                yield return [scenarioId, BridgeTicketMutationMode.None];
                yield return [scenarioId, BridgeTicketMutationMode.ModifyOnTarget];
            }
        }

        public static IEnumerable<object[]> SingleNodeScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.SingleNode);

        public static IEnumerable<object[]> TwoNodeConsistentScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.TwoNodeConsistent);

        public static IEnumerable<object[]> TwoNodeInconsistentScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.TwoNodeInconsistent);

        public static IEnumerable<object[]> ThreeNodeOneStaleScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.ThreeNodeOneStale);

        public static IEnumerable<object[]> ThreeNodeTwoStaleScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.ThreeNodeTwoStale);

        public static IEnumerable<object[]> ThreeNodeAllDifferentScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.ThreeNodeAllDifferent);

        public static IEnumerable<object[]> ThreeNodeTieScenarioMatrix() => BuildScenarioMatrix(ScenarioGroup.ThreeNodeTie);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(SingleNodeScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Single_Node_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(TwoNodeConsistentScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Two_Node_Consistent_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(TwoNodeInconsistentScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Two_Node_Inconsistent_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(ThreeNodeOneStaleScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Three_Node_One_Stale_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(ThreeNodeTwoStaleScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Three_Node_Two_Stale_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(ThreeNodeAllDifferentScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Three_Node_All_Different_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenMemberData(nameof(ThreeNodeTieScenarioMatrix), DatabaseMode = RavenDatabaseMode.Single)]
        public Task Should_Converge_On_Three_Node_Tie_Scenarios(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [ScenarioId.AllThreeAllDifferentCWins, BridgeTicketMutationMode.None], Skip = "Filtered pull replication is not supported on sharded databases.")]
        public Task Should_Not_Run_On_Sharded_Databases(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode) =>
            ExecuteScenarioAsync(options, scenarioId, mutationMode);
    }
}
