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

            var errors = commands.GetIndexErrors();

            Assert.Empty(errors.SelectMany(x => x.Errors));
        }
    }
}