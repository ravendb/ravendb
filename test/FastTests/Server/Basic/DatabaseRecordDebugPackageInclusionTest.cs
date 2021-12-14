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
            var sensitiveFieldsThatShouldNotBeExposedForDebug = new HashSet<string>()
            {
                nameof(DatabaseRecord.RavenConnectionStrings),
                nameof(DatabaseRecord.SqlConnectionStrings),
            };
            
            foreach (var safe in DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug)
            {
                Assert.False(sensitiveFieldsThatShouldNotBeExposedForDebug.Contains(safe),
                    $"Can't have the field '{safe}' in both {nameof(sensitiveFieldsThatShouldNotBeExposedForDebug)} and {nameof(DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug)}");
            }

            var allFields = DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug.Concat(sensitiveFieldsThatShouldNotBeExposedForDebug).ToHashSet();

            foreach (var prop in typeof(DatabaseRecord).GetFields())
            {
                Assert.True(allFields.Contains(prop.Name),
                    $"Field '{prop.Name}' in class {nameof(DatabaseRecord)} needs to be checked for being included in the debug package info at '{nameof(DatabaseRecordHandler.FieldsThatShouldBeExposedForDebug)}'");
            }
        }
    }
}
