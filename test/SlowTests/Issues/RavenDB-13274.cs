using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13274 : RavenTestBase
    {
        public RavenDB_13274(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_throw_on_attempt_to_put_index_with_name_that_exceeds_limit()
        {
            using (var store = GetDocumentStore())
            {
                var indexDef = new IndexDefinition
                {
                    Name = new string('a', IndexStore.MaxIndexNameLength),
                    Maps =
                    {
                        @"from o in docs.Orders
                          select new { Company = o.Company }"
                    }
                };

                // should not throw
                store.Maintenance.Send(new PutIndexesOperation(indexDef));

                var indexDef2 = new IndexDefinition
                {
                    Name = new string('b', IndexStore.MaxIndexNameLength + 1),
                    Maps =
                    {
                        @"from o in docs.Orders
                          select new { Lines = o.Lines }"
                    }
                };

                var ex = Assert.Throws<RavenException>(() => 
                    store.Maintenance.Send(new PutIndexesOperation(indexDef2)));

                Assert.Contains($"Index name cannot exceed {IndexStore.MaxIndexNameLength} characters.", ex.Message);
            }
        }

        [Fact]
        public void Should_throw_on_attempt_to_clone_index_with_new_name_that_exceeds_limit()
        {
            using (var store = GetDocumentStore())
            {
                var indexDef = new IndexDefinition
                {
                    Name = "Orders/ByComapny",
                    Maps =
                    {
                        @"from o in docs.Orders
                          select new { Company = o.Company }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDef));

                // cloned index should throw
                var indexDef2 = new IndexDefinition
                {
                    Name = new string('a', IndexStore.MaxIndexNameLength + 1),
                    Maps = indexDef.Maps
                };

                var ex = Assert.Throws<RavenException>(() =>
                    store.Maintenance.Send(new PutIndexesOperation(indexDef2)));

                Assert.Contains($"Index name cannot exceed {IndexStore.MaxIndexNameLength} characters.", ex.Message);

            }
        }

    }
}
