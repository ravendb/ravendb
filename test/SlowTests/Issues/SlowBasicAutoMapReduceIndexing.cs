using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Indexing.Auto;
using Xunit;

namespace SlowTests.Issues
{
    public class SlowBasicAutoMapReduceIndexing : NoDisposalNeeded
    {
        [Theory]
        [InlineData(50000, new[] {"Canada", "France"})] // reduce key tree with depth 3
        public async Task MultipleReduceKeys(int numberOfUsers, string[] locations)
        {
            using (var a = new BasicAutoMapReduceIndexing(Output))
            {
                await a.MultipleReduceKeys(numberOfUsers, locations);
            }
        }
    }
}