using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Raven.Database.Indexing;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs
{
    public class IndexDefinitionEquality
    {
        [Fact]
        public void TransformResultsFactoredIntoEqualityCheck()
        {
            IndexDefinition definitionOne = new IndexDefinition<Blog, Blog>()
            {
                Map = docs => from doc in docs
                              select new { doc.Property },
                TransformResults = (database, results) => from result in results
                                                          select new
                                                          {
                                                              Property = result.Property
                                                          }
            }.ToIndexDefinition(new Client.Document.DocumentConvention());

            IndexDefinition definitionTwo = new IndexDefinition<Blog, Blog>()
            {
                Map = docs => from doc in docs
                              select new { doc.Property }
            }.ToIndexDefinition(new Client.Document.DocumentConvention());

            Assert.False(definitionOne.Equals(definitionTwo));
        }

        public class Blog
        {
            public string Property { get; set; }
        }
    }
}
