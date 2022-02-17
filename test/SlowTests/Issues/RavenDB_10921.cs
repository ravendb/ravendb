using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10921 : RavenTestBase
    {
        public RavenDB_10921(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MustNotPutIndexWithReplacementOfPrefix()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<RavenException>(() =>
                {
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition()
                    {
                        Name = $"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}test",
                        Maps =
                    {
                        "from u in docs.Users select new { u.Age }"
                    }
                    }));
                });

                Assert.Contains($"Index name must not start with '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}'. Provided index name: '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}test'", ex.Message);
            }
        }
    }
}
