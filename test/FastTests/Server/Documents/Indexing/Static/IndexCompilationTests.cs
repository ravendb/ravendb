using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class IndexCompilationTests : NoDisposalNeeded
    {
        [Fact]
        public void Sum_of_elements()
        {
            IndexCompiler.Compile(new IndexDefinition
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
