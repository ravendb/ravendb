using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Documents.Indexing.Static;
using Xunit;

namespace SlowTests.Server.Documents.Indexing
{
    public class CollisionsOfReduceKeyHashes_StressTests : NoDisposalNeeded
    {
        [Theory]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Auto_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var test = new CollisionsOfReduceKeyHashes())
            {
                await test.Auto_index_should_produce_multiple_outputs(numberOfUsers, locations);
            }
        }

        [Theory]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Static_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var test = new CollisionsOfReduceKeyHashes())
            {
                await test.Static_index_should_produce_multiple_outputs(numberOfUsers, locations);
            }
        }
    }
}