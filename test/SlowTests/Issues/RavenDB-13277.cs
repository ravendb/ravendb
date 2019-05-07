using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13277: RavenTestBase
    {
        [Fact]
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
