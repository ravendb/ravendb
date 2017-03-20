using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
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