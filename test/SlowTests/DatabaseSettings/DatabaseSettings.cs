using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Server.Config;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.DatabaseSettings
{
    public class DatabaseSettings : NoDisposalNeeded
    {
        public DatabaseSettings(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckConfigurationEntriesTypesHandledByTheDatabaseSettingsViewInStudio()
        {
            var typesHandledByStudio = new HashSet<string> { "String", "Integer", "Double", "Enum", "Boolean", "Path", "Time", "Size" };
            var nullableTypesHandledByStudio = new HashSet<string> {"Integer", "Double", "Time", "Size"};
            
            const string itemNotHandledText = "are Not handled by the Database Settings View in the Studio. Pls contact @Danielle in order to add this item.";

            var entriesInDatabaseScope = RavenConfiguration.AllConfigurationEntriesForConfigurationNamesAndDebug.Value
                .Where(x => x.Scope.ToString() == "ServerWideOrPerDatabase").ToList();

            var entriesByType = entriesInDatabaseScope.GroupBy(x => x.Type,
                (key, g) => new { Type = key.ToString() });

            foreach (var item in entriesByType)
            {
                Assert.True(typesHandledByStudio.Contains(item.Type), $"Items with Type: '{item.Type}' {itemNotHandledText}");
            }

            var isNullableEntriesByType = entriesInDatabaseScope.Where(x => x.IsNullable)
                .GroupBy(x => x.Type,
                    (key, g) => new { Type = key.ToString() });

            foreach (var item in isNullableEntriesByType)
            {
                Assert.True(nullableTypesHandledByStudio.Contains(item.Type), $"isNullable items with Type: '{item.Type}' {itemNotHandledText}");
            }

            var isArrayEntriesByType = entriesInDatabaseScope.Where(x => x.IsArray)
                .GroupBy(x => x.Type,
                    (key, g) => new { Type = key.ToString() });

            foreach (var item in isArrayEntriesByType)
            {
                Assert.True(typesHandledByStudio.Contains(item.Type), $"isArray items with Type: '{item.Type}' {itemNotHandledText}");
            }
        }
    }
}
