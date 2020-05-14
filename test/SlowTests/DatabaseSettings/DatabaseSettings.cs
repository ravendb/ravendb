using System.Collections.Generic;
using FastTests;
using Raven.Server.Config;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using System.Linq;

namespace SlowTests.DataBaseSettings
{
    public class DatabaseSettings : RavenTestBase
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
            
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var entriesInDatabaseScope = RavenConfiguration.AllConfigurationEntries.Value
                    .Where(x => x.Scope.ToString() == "ServerWideOrPerDatabase");

                var entriesByType = entriesInDatabaseScope.GroupBy(x => x.Type,
                    (key, g) => new {Type = key.ToString()});
                
                foreach (var item in entriesByType)
                {
                    Assert.True(typesHandledByStudio.Contains(item.Type), $"Items with Type: '{item.Type}' {itemNotHandledText}");
                }

                var isNullableEntriesByType = entriesInDatabaseScope.Where(x => x.IsNullable)
                    .GroupBy(x => x.Type,
                        (key, g) => new {Type = key.ToString()});

                foreach (var item in isNullableEntriesByType)
                {
                    Assert.True(nullableTypesHandledByStudio.Contains(item.Type),$"isNullable items with Type: '{item.Type}' {itemNotHandledText}");
                }
                
                var isArrayEntriesByType = entriesInDatabaseScope.Where(x => x.IsArray)
                    .GroupBy(x => x.Type,
                        (key, g) => new {Type = key.ToString()});

                foreach (var item in isArrayEntriesByType)
                {
                    Assert.True(typesHandledByStudio.Contains(item.Type),$"isArray items with Type: '{item.Type}' {itemNotHandledText}");
                }
            }
        }
    }
}
