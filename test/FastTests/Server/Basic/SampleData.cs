using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class SampleData : RavenTestBase
    {
        public SampleData(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler)]
        public async Task CanCreateSampleData()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());
            }
        }
    }
}
