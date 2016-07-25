using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class IndexCompilationTests
    {
        [Fact]
        public void Sum_of_elements()
        {
            IndexAndTransformerCompiler.Compile(new IndexDefinition
            {
                Name = "test",
                Maps =
                {
                    @"from order in docs.Orders select new { 
                            order.Company, 
                            Count = 1, 
                            Total = order.Lines.Sum(l => l.PricePerUnit)
                    }"
                }
            });
        }
    }
}