using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295
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
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [ScenarioId.AllThreeAllDifferentCWins, BridgeTicketMutationMode.None], Skip = "Filtered pull replication is not supported on sharded databases.")]
        public async Task Should_Converge_On_All_Nodes(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode)
        {
            await using var lab = await ReplicationLab.CreateAsync(owner: this, options, ScenarioCatalog.Definitions.Value[scenarioId], mutationMode);

            ScenarioExecutionReport report = null;
            try
            {
                report = await lab.RunUntilStateVerifiedAsync();
                lab.AssertVerifiedState(report);
                await lab.VerifyReplicationAliveAfterVerificationAsync();
            }
            catch (Exception e)
            {
                report ??= await lab.CaptureReportAsync();
                throw new Xunit.Sdk.XunitException(ScenarioFailureReportBuilder.Build(report, e));
            }
        }
    }
}
