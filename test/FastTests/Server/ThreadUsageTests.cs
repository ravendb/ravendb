using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server
{
    public class ThreadUsageTests : RavenLowLevelTestBase
    {
        public ThreadUsageTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ThreadUsage_WhenThreadsHaveSameCpuUsageAndTotalProcessorTime_ShouldListThemBoth()
        {
            using (var database = CreateDocumentDatabase())
            {
                using var index1 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Companies", new[] { new AutoIndexField { Name = "Name" } }), Guid.NewGuid().ToString());
                using var index2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Age" } }), Guid.NewGuid().ToString());
                {
                    var threadsUsage = new ThreadsUsage();
                    var threadsInfo = threadsUsage.Calculate();
                    var threadNames = threadsInfo.List.Select(ti => ti.Name).ToArray();
                    Assert.Contains(index1._indexingThread.Name, threadNames);
                    Assert.Contains(index2._indexingThread.Name, threadNames);
                }
            }
        }
    }
}
