using System.Linq;
using Raven.Client;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Utils
{
    public class TestHelper
    {
        public static void AssertNoIndexErrors(Raven.NewClient.Client.IDocumentStore store, string databaseName = null)
        {
            var errors = store.Admin.ForDatabase(databaseName).Send(new GetIndexErrorsOperation());

            Assert.Empty(errors.SelectMany(x => x.Errors));
        }

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