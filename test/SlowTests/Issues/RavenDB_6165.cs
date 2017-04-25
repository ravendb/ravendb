using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions.Cluster;
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

        private class Users_FullName_Transformer : AbstractTransformerCreationTask<User>
        {
            public Users_FullName_Transformer()
            {
                TransformResults = users => from user in users select new { FullName = user.Name + " " + user.LastName };
            }
        }

        [Theory(Skip = "RavenDB-6820")]
        [InlineData("my-index", "0001-my-index")]
        [InlineData("Users/ByNameRenamed", "0001-Users_ByNameRenamed")]
        public async Task Can_rename_index(string newIndexName, string newIndexDirName)
        {
            var path = NewDataPath();
            long indexEtag;
            using (var documentStore = GetDocumentStore(path: path))
            {
                var usersByName = new Users_ByName();
                usersByName.Execute(documentStore);

                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new RenameIndexOperation(usersByName.IndexName, newIndexName));

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(newIndexName, stats.Indexes[0].Name);
                indexEtag = stats.Indexes[0].Etag;

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(documentStore.DefaultDatabase);

                var database = await GetDatabase(documentStore.DefaultDatabase);

                var index = database.IndexStore.GetIndex(indexEtag);

                var envPath = index._indexStorage.Environment().Options.BasePath;

                Assert.Equal(new DirectoryInfo(envPath).Name, newIndexDirName);

                 stats = documentStore.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(newIndexName, stats.Indexes[0].Name);
                Assert.Equal(indexEtag, stats.Indexes[0].Etag);
            }
        }

        [Fact]
        public void Cannot_rename_index_if_there_is_index_or_transformer_having_the_same_name()
        {
            var path = NewDataPath();

            using (var documentStore = GetDocumentStore(path: path))
            {
                var usersByName = new Users_ByName();
                usersByName.Execute(documentStore);

                var existingIndexName = "Users_ByName_Exists";

                documentStore
                    .Admin
                    .Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = existingIndexName,
                        Maps = {"from user in docs.Users select new { user.Name }"},
                        Type = IndexType.Map
                    }));

                var existingTransformerName = "Users_Transformer";

                documentStore.Admin.Send(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = existingTransformerName,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                WaitForIndexing(documentStore);

                Assert.Throws<IndexOrTransformerAlreadyExistException>(
                    () => documentStore.Admin.Send(new RenameIndexOperation(usersByName.IndexName, existingIndexName)));

                Assert.Throws<IndexOrTransformerAlreadyExistException>(
                   () => documentStore.Admin.Send(new RenameIndexOperation(usersByName.IndexName, existingTransformerName)));
            }
        }

        [Theory]
        [InlineData("my-transformer")]
        public async Task Can_rename_transformer(string newTransformerName)
        {
            var path = NewDataPath();

            using (var documentStore = GetDocumentStore(path: path))
            {
                var usersTransformer = new Users_FullName_Transformer();
                usersTransformer.Execute(documentStore);

                var etag = documentStore.Admin.Send(new RenameTransformerOperation(usersTransformer.TransformerName, newTransformerName));

                await Server.ServerStore.Cluster.WaitForIndexNotification(etag);

                var database = await GetDatabase(documentStore.DefaultDatabase);

                var transformer = database.TransformerStore.GetTransformer(newTransformerName);

                Assert.Equal(newTransformerName, transformer.Name);
                Assert.Equal(1, database.TransformerStore.GetTransformers().Count());

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(documentStore.DefaultDatabase, null);

                database = await GetDatabase(documentStore.DefaultDatabase);

                transformer = database.TransformerStore.GetTransformer(newTransformerName);

                Assert.Equal(newTransformerName, transformer.Name);
                Assert.Equal(1, database.TransformerStore.GetTransformers().Count());
            }
        }

        [Fact]
        public void Cannot_rename_transformer_if_there_is_index_or_transformer_having_the_same_name()
        {
            var path = NewDataPath();

            using (var documentStore = GetDocumentStore(path: path))
            {
                var usersTransformer = new Users_FullName_Transformer();
                usersTransformer.Execute(documentStore);

                var existingIndexName = "Users_ByName_Exists";

                documentStore
                    .Admin
                    .Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = existingIndexName,
                        Maps = { "from user in docs.Users select new { user.Name }" },
                        Type = IndexType.Map
                    }));

                var existingTransformerName = "Users_Transformer";

                documentStore.Admin.Send(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = existingTransformerName,
                    TransformResults = "from user in results select new { user.FirstName, user.LastName }"
                }));

                WaitForIndexing(documentStore);

                Assert.Throws<CommandExecutionException>(
                    () => documentStore.Admin.Send(new RenameTransformerOperation(usersTransformer.TransformerName, existingIndexName)));

                Assert.Throws<CommandExecutionException>(
                   () => documentStore.Admin.Send(new RenameTransformerOperation(usersTransformer.TransformerName, existingTransformerName)));
            }
        }
    }
}