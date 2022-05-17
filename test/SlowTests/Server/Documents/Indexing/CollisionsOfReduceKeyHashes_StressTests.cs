using Tests.Infrastructure;
using System.Threading.Tasks;
using SlowTests.Server.Documents.Indexing.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class CollisionsOfReduceKeyHashes_StressTests : NoDisposalNoOutputNeeded
    {
        public CollisionsOfReduceKeyHashes_StressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Auto_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var test = new CollisionsOfReduceKeyHashes(Output))
            {
                await test.Auto_index_should_produce_multiple_outputs(numberOfUsers, locations);
            }
        }

        [Theory]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Static_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var test = new CollisionsOfReduceKeyHashes(Output))
            {
                await test.Static_index_should_produce_multiple_outputs(numberOfUsers, locations);
            }
        }
    }
}
