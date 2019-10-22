using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8220 : RavenTestBase
    {
        public RavenDB_8220(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGenerateIndexesWithTheSame64CharPrefix()
        {
            using (var store = GetDocumentStore())
            {
                await store.Commands().QueryAsync(new IndexQuery { Query = @"
from items 
where Map.Nested.Key = 'Color'  and Map.Nested.Color = 'Blue'
" });
                await store.Commands().QueryAsync(new IndexQuery { Query = @"
from items 
where Map.Nested.Key = 'Color'  and Map.Value.Nested.Color = 'Blue'

" });
                await store.Commands().QueryAsync(new IndexQuery { Query = @"
from items 
where Map.Value.Nested.Key = 'Color'  and Map.Value.Nested.Value.Color = 'Blue'
" });
                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10)).OrderBy(x => x.Name.Length).ToList();

                Assert.Equal(3, indexes.Count);
                Assert.Equal(indexes[1].Name.Substring(0, 64), indexes[2].Name.Substring(0, 64));


            }
        }
    }
}
