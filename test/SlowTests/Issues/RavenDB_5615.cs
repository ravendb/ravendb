using FastTests;
using Raven.Client.Data.Indexes;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5615 : RavenTestBase
    {
        [Fact]
        public void CanEnableAndDisableIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByCity();
                index.Execute(store);

                var indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Normal, indexStats.State);
                Assert.Equal(IndexRunningStatus.Running, indexStats.Status);

                store.DatabaseCommands.Admin.EnableIndex(index.IndexName); // no-op

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Normal, indexStats.State);
                Assert.Equal(IndexRunningStatus.Running, indexStats.Status);

                store.DatabaseCommands.Admin.DisableIndex(index.IndexName);

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Disabled, indexStats.State);
                Assert.Equal(IndexRunningStatus.Disabled, indexStats.Status);

                store.DatabaseCommands.Admin.DisableIndex(index.IndexName); // no-op

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Disabled, indexStats.State);
                Assert.Equal(IndexRunningStatus.Disabled, indexStats.Status);

                store.DatabaseCommands.Admin.StartIndex(index.IndexName); // cannot start disabled index

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Disabled, indexStats.State);
                Assert.Equal(IndexRunningStatus.Disabled, indexStats.Status);

                store.DatabaseCommands.Admin.EnableIndex(index.IndexName);

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexState.Normal, indexStats.State);
                Assert.Equal(IndexRunningStatus.Running, indexStats.Status);
            }
        }

        [Fact]
        public void CanChangeIndexPriority()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByCity();
                index.Execute(store);

                var indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexPriority.Normal, indexStats.Priority);

                store.DatabaseCommands.SetIndexPriority(index.IndexName, IndexPriority.Normal); // no-op

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexPriority.Normal, indexStats.Priority);

                store.DatabaseCommands.SetIndexPriority(index.IndexName, IndexPriority.Low);

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexPriority.Low, indexStats.Priority);

                store.DatabaseCommands.SetIndexPriority(index.IndexName, IndexPriority.High);

                indexStats = store.DatabaseCommands.GetIndexStatistics(index.IndexName);
                Assert.Equal(IndexPriority.High, indexStats.Priority);
            }
        }
    }
}