using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Documents.Replication;
using Raven.Client.Indexes;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationIndexesAndTransformers : ReplicationTestsBase
    {
        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        public class UserByName : AbstractIndexCreationTask<User>
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

        public class UserByAge : AbstractIndexCreationTask<User>
        {
            public UserByAge()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        [Fact]
        public async Task Creating_indexes_should_create_relevant_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAge();
                var userByName = new UserByName();
                userByName.Execute(store);
                userByAge.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                var metadataByName = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByName.IndexName);
                Assert.NotNull(metadataByName);

                var serversideIndexMetadata = databaseStore.IndexStore.GetIndex(userByName.IndexName);
                Assert.Equal(serversideIndexMetadata.IndexId, metadataByName.Id);

                var metadataByAge = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByAge.IndexName);
                Assert.NotNull(metadataByAge);

                serversideIndexMetadata = databaseStore.IndexStore.GetIndex(userByName.IndexName);
                Assert.Equal(serversideIndexMetadata.IndexId, metadataByAge.Id);
            }
        }
    }
}
