using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
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
        public void ThreadUsage_WhenThreadsHaveSameCpuUsageAndTotalProcessorTime_ShouldListThemBoth()
        {
            using var database = CreateDocumentDatabase();

            using var index1 = MapIndex.CreateNew(new IndexDefinition {Name = "Companies_ByName", Maps = { "from company in docs.Companies select new { company.Name }" },}, database);
            using var index2 = MapIndex.CreateNew(new IndexDefinition {Name = "Users_ByName", Maps = {"from user in docs.Orders select new { user.Name }"},}, database);
            using var index3 = MapIndex.CreateNew(new IndexDefinition {Name = "Orders_ByName", Maps = { "from order in docs.Orders select new { order.Name }" },}, database);

            index1.Start();
            index2.Start();
            index3.Start();

            var threadsUsage = new ThreadsUsage();
            var threadsInfo = threadsUsage.Calculate();
            var threadNames = threadsInfo.List.Select(ti => ti.Name).OrderBy(n => n).ToArray();

            RavenTestHelper.AssertAll(() => string.Join('\n', threadNames.Select(s => $"\"{s}\"")),
                () => AssertContains(index1._indexingThread.Name),
                () => AssertContains(index2._indexingThread.Name),
                () => AssertContains(index3._indexingThread.Name));

            void AssertContains(string threadName) => Assert.True(threadNames.Contains(threadName), $"Not found : {threadName}");
        }
    }
}
