using System.Threading.Tasks;
using FastTests;
using Raven.Server.SqlMigration;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class ListDatabasesTest : SqlAwareTestBase
    {
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        public Task CanListDatabaseNames(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
             
                var dbNames = driver.GetDatabaseNames();
                
                // it should contain at least current database
                Assert.True(dbNames.Count > 0);
                
                return Task.CompletedTask;
            }
        }
        
    }
}
