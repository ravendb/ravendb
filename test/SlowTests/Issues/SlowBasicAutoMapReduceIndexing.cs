using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing.Auto;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class SlowBasicAutoMapReduceIndexing : NoDisposalNoOutputNeeded
    {
        public SlowBasicAutoMapReduceIndexing(ITestOutputHelper output) : base(output)
        {
        }

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
