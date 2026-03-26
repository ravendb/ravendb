using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13277: RavenTestBase
    {
        public RavenDB_13277(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void CreateIndexWithTheSameLowercasedNameShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var indexDef = new IndexDefinition
                {
                    Name = "A",
                    Maps =
                    {
                        @"from o in docs.Orders
                          select new { Company = o.Company }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDef));
                indexDef.Name = "a";
                Assert.Throws<IndexCreationException>(()=>store.Maintenance.Send(new PutIndexesOperation(indexDef)));                
            }
        }
    }
}
