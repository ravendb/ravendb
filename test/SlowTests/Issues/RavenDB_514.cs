using FastTests;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_514 : RavenNewTestBase
    {
        [Fact]
        public void BoostWithLinq()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "from p in docs.Products select new { p.Price} .Boost(2)" }
                }));
            }
        }

        [Fact]
        public void BoostWithMethod()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "docs.Products.Select(p =>new { p.Price } .Boost(2))" }
                }));
            }
        }
    }
}
