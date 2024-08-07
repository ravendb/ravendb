using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class IndexCompilationTests : NoDisposalNeeded
    {
        public IndexCompilationTests(ITestOutputHelper output) : base(output)
        {
        }

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
            }, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
        }
    }
}
