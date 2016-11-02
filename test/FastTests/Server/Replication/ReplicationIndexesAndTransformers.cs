using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using Raven.Client.Indexes;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationIndexesAndTransformers : ReplicationTestsBase
    {
        public class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public int Age { get; set; }
        }

        public class UsersByLastnameIndex : AbstractIndexCreationTask<User>
        {
            public UsersByLastnameIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.LastName
                    };
            }
        }

        public class UsersByAgeIndex : AbstractIndexCreationTask<User>
        {
            public UsersByAgeIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        [Fact]
        public async Task Created_index_has_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var usersByLastnameIndex = new UsersByLastnameIndex();
                usersByLastnameIndex.Execute(store);

                var usersByAgeIndex = new UsersByAgeIndex();
                usersByAgeIndex.Execute(store);

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
                using (var tx = database.DocumentsStorage.Environment.ReadTransaction())
                {
                    var metadata = database.IndexTransformerMetadataStorage.GetMetadataAfter(0, MetadataStorageType.Index, tx).ToList();
                    Assert.Equal(2,metadata.Count);                    
                }
            }
        }

        [Fact]
        public void Can_replicate_single_index_with_single_source_single_destination()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destination);

                var usersByLastnameIndex = new UsersByLastnameIndex();
                usersByLastnameIndex.Execute(source);

                Assert.True(WaitForIndexToReplicate(destination, usersByLastnameIndex.IndexName, 2000));
            }
        }
    }
}

