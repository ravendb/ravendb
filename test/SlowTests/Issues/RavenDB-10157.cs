using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10157 : RavenTestBase
    {
        [Fact]
        public async Task CanExportIndexesCorrectly()
        {
            DoNotReuseServer();

            var path = NewDataPath(forceCreateDir: true);
            var exportFile = Path.Combine(path, "export.ravendbdump");

            using (var store = GetDocumentStore())
            {
                new UserByName().Execute(store);
                new UserByNameCount().Execute(store);
                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = GetDocumentStore())
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfIndexes);
                Assert.Equal(IndexType.Map, stats.Indexes.First(x => x.Name.Equals(new UserByName().IndexName)).Type);
                Assert.Equal(IndexType.MapReduce, stats.Indexes.First(x => x.Name.Equals(new UserByNameCount().IndexName)).Type);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class UserByName : AbstractIndexCreationTask<User>
        {
            public UserByName()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        private class UserCount
        {
            public string Name { get; set; }

            public int Count { get; set; }
        }

        private class UserByNameCount : AbstractIndexCreationTask<User, UserCount>
        {
            public UserByNameCount()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name,
                        Count = 1
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Name
                    into g
                    select new
                    {
                        Name = g.Key,
                        Count = g.Sum(x => x.Count)
                    };
            }
        }
    }
}
