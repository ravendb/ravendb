using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing.Auto;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class SlowBasicAutoMapReduceIndexing : NoDisposalNoOutputNeeded
    {
        public SlowBasicAutoMapReduceIndexing(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(50000, new[] {"Canada", "France"})] // reduce key tree with depth 3
        public async Task MultipleReduceKeys(int numberOfUsers, string[] locations)
        {
            await using (var a = new BasicAutoMapReduceIndexing(Output))
            {
                await a.MultipleReduceKeys(numberOfUsers, locations);
            }
        }
    }
}
