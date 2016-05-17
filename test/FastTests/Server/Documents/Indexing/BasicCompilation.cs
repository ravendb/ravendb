using System.Collections.Generic;

using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;

using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicCompilation : RavenTestBase
    {
        [Fact]
        public void T1()
        {
            var compiler = new StaticIndexCompiler();
            var indexDefinition = new IndexDefinition();
            indexDefinition.Name = "Orders_ByName";
            indexDefinition.Maps = new HashSet<string>
            {
                 "from order in docs.Orders select new { order.Name };"
            };

            compiler.Compile(indexDefinition);
        }
    }
}