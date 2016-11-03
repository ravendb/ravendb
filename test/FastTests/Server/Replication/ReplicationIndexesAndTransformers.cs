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
            public DateTime Birthday { get; set; }
        }

        public class UserByNameAndBirthday : AbstractIndexCreationTask<User>
        {
            public UserByNameAndBirthday()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.Birthday
                               };
            }
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
        public async Task Deleting_indexes_should_delete_relevant_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAge();
                var userByName = new UserByName();
                userByName.Execute(store);
                userByAge.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                store.DatabaseCommands.DeleteIndex(userByName.IndexName);

                var metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByAge.IndexName);
                Assert.NotNull(metadata);

                metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByName.IndexName);
                Assert.Null(metadata);

                store.DatabaseCommands.DeleteIndex(userByAge.IndexName);
                metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByAge.IndexName);
                Assert.Null(metadata);
            }
        }

        [Fact]
        public async Task PurgeTombstonesFrom_should_work_properly()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAge();
                var userByName = new UserByName();
                var userByNameAndBirthday = new UserByNameAndBirthday();
                userByName.Execute(store);
                userByAge.Execute(store);
                userByNameAndBirthday.Execute(store);

                store.DatabaseCommands.DeleteIndex(userByName.IndexName);                
                store.DatabaseCommands.DeleteIndex(userByNameAndBirthday.IndexName);

                var databaseStore = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
                databaseStore.IndexMetadataPersistence.PurgeTombstonesFrom(0,1024);

                var metadataCollection = databaseStore.IndexMetadataPersistence.GetAfter(0, 0, 1024);
                Assert.Equal(1,metadataCollection.Count);
                Assert.Equal(userByAge.IndexName.ToLower(),metadataCollection[0].Name);
            }
        }

        [Fact]
        public async Task GetAfter_should_work_properly()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAge();
                var userByName = new UserByName();
                var userByNameAndBirthday = new UserByNameAndBirthday();
                userByName.Execute(store);
                userByAge.Execute(store);
                userByNameAndBirthday.Execute(store);

                var databaseStore = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
                var metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(0, 0, 1024);
                Assert.Equal(3,metadataItems.Count);

                metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(3, 0, 1024);
                Assert.Equal(1, metadataItems.Count);

                //this one was created last, so it has the largest etag
                Assert.Equal(userByNameAndBirthday.IndexName.ToLower(),metadataItems[0].Name);

                store.DatabaseCommands.DeleteIndex(userByName.IndexName);
                store.DatabaseCommands.DeleteIndex(userByNameAndBirthday.IndexName);

                metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(0, 0, 1024);
                Assert.Equal(3, metadataItems.Count); //together with tombstones
                Assert.Equal(2, metadataItems.Count(item => item.Id == -1));
            }
        }

        [Fact]
        public async Task Deleting_indexes_should_write_tombstones()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAge();
                var userByName = new UserByName();
                userByName.Execute(store);
                userByAge.Execute(store);

                store.DatabaseCommands.DeleteIndex(userByName.IndexName);
                store.DatabaseCommands.DeleteIndex(userByAge.IndexName);
                var databaseStore = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                var metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByAge.IndexName, returnNullIfTombstone: false);
                Assert.NotNull(metadata);
                Assert.Equal(-1,metadata.Id);
                Assert.Equal(userByAge.IndexName.ToLower(),metadata.Name);

                metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(userByName.IndexName, returnNullIfTombstone: false);
                Assert.NotNull(metadata);
                Assert.Equal(-1, metadata.Id);
                Assert.Equal(userByName.IndexName.ToLower(), metadata.Name);

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

                serversideIndexMetadata = databaseStore.IndexStore.GetIndex(userByAge.IndexName);
                Assert.Equal(serversideIndexMetadata.IndexId, metadataByAge.Id);
            }
        }
    }
}
