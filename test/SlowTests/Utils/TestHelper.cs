using System.Linq;
using Raven.NewClient.Client;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Utils
{
    public class TestHelper
    {
        public static void AssertNoIndexErrors(IDocumentStore store, string databaseName = null)
        {
            var errors = store.Admin.ForDatabase(databaseName).Send(new GetIndexErrorsOperation());

            Assert.Empty(errors.SelectMany(x => x.Errors));
        }
    }
}