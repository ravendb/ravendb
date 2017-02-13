using FastTests;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
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
                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Name = "test",
                    Maps = { "from p in docs.Products select new { p.Price} .Boost(2)" }
                }}));
            }
        }

        [Fact]
        public void BoostWithMethod()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "test",
                    Maps = { "docs.Products.Select(p =>new { p.Price } .Boost(2))" }
                }}));
            }
        }
    }
}
