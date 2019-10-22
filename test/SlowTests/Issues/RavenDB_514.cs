using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_514 : RavenTestBase
    {
        public RavenDB_514(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BoostWithLinq()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
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
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "test",
                    Maps = { "docs.Products.Select(p =>new { p.Price } .Boost(2))" }
                }}));
            }
        }
    }
}
