using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13387 : RavenLowLevelTestBase
    {
        public RavenDB_13387(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_leave_low_memory_mode()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                }, database))
                {
                    index.SimulateLowMemory();

                    Assert.True(index.IsLowMemory);

                    for (var i = 0; i < Raven.Server.Documents.Indexes.Index.LowMemoryPressure; i++)
                    {
                        index.LowMemoryOver();
                    }

                    Assert.False(index.IsLowMemory);
                }
            }
        }
    }
}
