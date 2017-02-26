using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6165 : RavenTestBase
    {
        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users select new {user.Name};
            }
        }

        [Fact]
        public async Task Can_rename_index()
        {
            var path = NewDataPath();

            using (var documentStore = GetDocumentStore(path: path))
            {
                var usersByName = new Users_ByName();
                usersByName.Execute(documentStore);

                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new RenameIndexOperation(usersByName.IndexName, "my-index"));

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());

                Assert.Equal("my-index", stats.Indexes[0].Name);
                Assert.Equal(1, stats.Indexes[0].IndexId);
            }

            using (var documentStore = GetDocumentStore(path: path))
            {
                var database = await GetDatabase(documentStore.DefaultDatabase);

                var index = database.IndexStore.GetIndex(1);

                var envPath = index._indexStorage.Environment().Options.BasePath;

                Assert.True(envPath.EndsWith("my-index"));

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());

                Assert.Equal("my-index", stats.Indexes[0].Name);
                Assert.Equal(1, stats.Indexes[0].IndexId);
            }
        }
    }
}