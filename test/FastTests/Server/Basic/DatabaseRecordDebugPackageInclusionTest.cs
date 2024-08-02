using System.Collections.Generic;
using System.Linq;
using Amqp.Framing;
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
            var sensitiveFieldsThatShouldNotBeExposedForDebug = new HashSet<string>()
            {
                nameof(DatabaseRecord.RavenConnectionStrings),
                nameof(DatabaseRecord.SqlConnectionStrings),
                nameof(DatabaseRecord.Settings),
                nameof(DatabaseRecord.PeriodicBackups),
                nameof(DatabaseRecord.ExternalReplications),
                nameof(DatabaseRecord.SinkPullReplications),
                nameof(DatabaseRecord.HubPullReplications),
                nameof(DatabaseRecord.RavenEtls),
                nameof(DatabaseRecord.SqlEtls),
                nameof(DatabaseRecord.OlapConnectionStrings),
                nameof(DatabaseRecord.OlapEtls),
                nameof(DatabaseRecord.Integrations),
                nameof(DatabaseRecord.ElasticSearchConnectionStrings),
                nameof(DatabaseRecord.ElasticSearchEtls),
                nameof(DatabaseRecord.QueueConnectionStrings),
                nameof(DatabaseRecord.QueueEtls),
                nameof(DatabaseRecord.QueueSinks),
                nameof(DatabaseRecord.SnowflakeConnectionStrings),
                nameof(DatabaseRecord.SnowflakeEtls),
            };
            
            foreach (var safe in ServerWideDebugInfoPackageHandler.FieldsThatShouldBeExposedForDebug)
            {
                Assert.False(sensitiveFieldsThatShouldNotBeExposedForDebug.Contains(safe),
                    $"Can't have the field '{safe}' in both {nameof(sensitiveFieldsThatShouldNotBeExposedForDebug)} and {nameof(ServerWideDebugInfoPackageHandler.FieldsThatShouldBeExposedForDebug)}");
            }

            var allFields = ServerWideDebugInfoPackageHandler.FieldsThatShouldBeExposedForDebug.Concat(sensitiveFieldsThatShouldNotBeExposedForDebug).ToHashSet();

            foreach (var prop in typeof(DatabaseRecord).GetFields())
            {
                Assert.True(allFields.Contains(prop.Name),
                    $"Field '{prop.Name}' in class {nameof(DatabaseRecord)} needs to be checked for being included in the debug package info at '{nameof(ServerWideDebugInfoPackageHandler.FieldsThatShouldBeExposedForDebug)}'");
            }
        }
    }
}
