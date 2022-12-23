using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using SlowTests.Server.Documents.Indexing.Static;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class CollisionsOfReduceKeyHashes_StressTests : NoDisposalNoOutputNeeded
    {
        public CollisionsOfReduceKeyHashes_StressTests(ITestOutputHelper output) : base(output)
        {
        }

        private static IEnumerable<object[]> Data() => new[]
        {
            new[] { new CollisionsOfReduceKeyHashes.TestData() {NumberOfUsers = 50000, Locations = new[] {"Canada", "France"}, SearchEngineType = SearchEngineType.Lucene}},
            new[] { new CollisionsOfReduceKeyHashes.TestData() {NumberOfUsers = 50000, Locations = new[] {"Canada", "France"}, SearchEngineType = SearchEngineType.Corax}}
        };
        
        [Theory] 
        [MemberData(nameof(Data))]// reduce key tree with depth 3
        public async Task Auto_index_should_produce_multiple_outputs(CollisionsOfReduceKeyHashes.TestData data)
        {
            using (var test = new CollisionsOfReduceKeyHashes(Output))
            {
                await test.Auto_index_should_produce_multiple_outputs(data);
            }
        }
        [Theory] 
        [MemberData(nameof(Data))]// reduce key tree with depth 3
        public async Task Static_index_should_produce_multiple_outputs(CollisionsOfReduceKeyHashes.TestData data)
        {
            using (var test = new CollisionsOfReduceKeyHashes(Output))
            {
                await test.Static_index_should_produce_multiple_outputs(data);
            }
        }
    }
}
