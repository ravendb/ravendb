using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Debugging;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class DatabaseRecordDebugPackageInclusionTest : RavenTestBase
    {
        public DatabaseRecordDebugPackageInclusionTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EnsureIncludedInDatabaseRecordDebugPackage()
        {
            // Only add to this list after deciding whether the new property should be exposed to the debug package
            // If it should be added to the debug package, add it to DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug
            var decidedFields = new HashSet<string>() {
                nameof(DatabaseRecord.DatabaseName),
                nameof(DatabaseRecord.Encrypted),
                nameof(DatabaseRecord.Disabled),
                nameof(DatabaseRecord.EtagForBackup),
                nameof(DatabaseRecord.DeletionInProgress),
                nameof(DatabaseRecord.DatabaseState),
                nameof(DatabaseRecord.Topology),
                nameof(DatabaseRecord.ConflictSolverConfig),
                nameof(DatabaseRecord.Sorters),
                nameof(DatabaseRecord.Indexes),
                nameof(DatabaseRecord.IndexesHistory),
                nameof(DatabaseRecord.AutoIndexes),
                nameof(DatabaseRecord.Settings),
                nameof(DatabaseRecord.Revisions),
                nameof(DatabaseRecord.RevisionsForConflicts),
                nameof(DatabaseRecord.Expiration),
                nameof(DatabaseRecord.Refresh),
                nameof(DatabaseRecord.PeriodicBackups),
                nameof(DatabaseRecord.ExternalReplications),
                nameof(DatabaseRecord.SinkPullReplications),
                nameof(DatabaseRecord.HubPullReplications),
                nameof(DatabaseRecord.RavenConnectionStrings),
                nameof(DatabaseRecord.SqlConnectionStrings),
                nameof(DatabaseRecord.RavenEtls),
                nameof(DatabaseRecord.SqlEtls),
                nameof(DatabaseRecord.Client),
                nameof(DatabaseRecord.Studio),
                nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount),
                nameof(DatabaseRecord.UnusedDatabaseIds) };

            foreach (var prop in typeof(DatabaseRecord).GetFields())
            {
                Assert.True(decidedFields.Contains(prop.Name),
                    $"Field '{prop.Name}' in class {nameof(DatabaseRecord)} needs to be checked for being included in the debug package info at '{nameof(DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug)}'");
            }
        }
    }
}
