using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace SlowTests.Utils
{
    public class TestHelper
    {
        public static void AssertNoIndexErrors(DocumentStore store, string databaseName = null)
        {
            var commands = string.IsNullOrEmpty(databaseName)
                ? store.DatabaseCommands
                : store.DatabaseCommands.ForDatabase(databaseName);

            Assert.Empty(commands.GetIndexErrors().SelectMany(x => x.Errors));
        }
    }
}