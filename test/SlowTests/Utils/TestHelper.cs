using System.Linq;
using Raven.Client;
using Xunit;

namespace SlowTests.Utils
{
    public class TestHelper
    {
        public static void AssertNoIndexErrors(IDocumentStore store, string databaseName = null)
        {
            var commands = string.IsNullOrEmpty(databaseName)
                ? store.DatabaseCommands
                : store.DatabaseCommands.ForDatabase(databaseName);

            var errors = commands.GetIndexErrors();

            Assert.Empty(errors.SelectMany(x => x.Errors));
        }
    }
}