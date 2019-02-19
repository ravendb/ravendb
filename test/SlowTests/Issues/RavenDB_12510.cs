using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12510: RavenTestBase
    {
        
        [Fact]
        public void MixedIndexSyntaxTypesInMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new IndexDefinition
                {
                    Name = "myIndex",
                    Maps = new HashSet<string>
                    {
                        "docs.Dogs.Select(dog => new { Id = Id(dog)})",
                        "from cat in docs.Cats select new { Id = Id(cat)}"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(index));

                
            }
        }
    }
}
