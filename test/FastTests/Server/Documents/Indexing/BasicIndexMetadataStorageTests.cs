using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Issues;
using Raven.Client.Indexes;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicIndexMetadataStorageTests : RavenTestBase
    {
        public class BasicTransformer : AbstractTransformerCreationTask<User>
        {
            public BasicTransformer()
            {
                TransformResults = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }


        public class BasicIndexA : AbstractIndexCreationTask<User>
        {
            public BasicIndexA()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        public class BasicIndexB : AbstractIndexCreationTask<User>
        {
            public BasicIndexB()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }
        public class BasicIndexC : AbstractIndexCreationTask<User>
        {
            public BasicIndexC()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        [Fact]
        public async Task Writing_indexes_transformers_and_tombstones_should_share_etags()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            {
                new BasicIndexA().Execute(store);
                new BasicTransformer().Execute(store);
                var indexes = documentDatabase.IndexStore.GetIndexes().ToList();
                documentDatabase
                    .IndexTransformerMetadataStorage
                    .WriteNewTombstoneFor(indexes[0].IndexId, MetadataStorageType.Index);

                using(var tx = documentDatabase.IndexTransformerMetadataStorage.Environment.ReadTransaction())
                    Assert.Equal(3,documentDatabase.IndexTransformerMetadataStorage.ReadLastEtag(tx));

                new BasicIndexB().Execute(store);

                using (var tx = documentDatabase.IndexTransformerMetadataStorage.Environment.ReadTransaction())
                    Assert.Equal(4, documentDatabase.IndexTransformerMetadataStorage.ReadLastEtag(tx));
            }
        }     

        [Fact]
        public async Task Read_and_write_index_tombstones_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            {
                new BasicIndexA().Execute(store);
                new BasicIndexB().Execute(store);
                new BasicIndexC().Execute(store);
                var indexes = documentDatabase.IndexStore.GetIndexes().ToList();
                documentDatabase
                    .IndexTransformerMetadataStorage
                    .WriteNewTombstoneFor(indexes[0].IndexId, MetadataStorageType.Index);
                documentDatabase
                    .IndexTransformerMetadataStorage
                    .WriteNewTombstoneFor(indexes[1].IndexId, MetadataStorageType.Index);
                documentDatabase
                    .IndexTransformerMetadataStorage
                    .WriteNewTombstoneFor(indexes[2].IndexId, MetadataStorageType.Index);

                var fetchedTombstones = 
                    documentDatabase.
                        IndexTransformerMetadataStorage.
                            GetTombstonesAfter(0).ToList();

                Assert.Equal(3,fetchedTombstones.Count);
                Assert.Equal(new[] {1L,2L,3L}, fetchedTombstones.Select(x => x.DeletedEtag));
            }
        }

        [Fact]
        public async Task Read_and_write_index_metadata_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new MetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize(documentDatabase.DocumentsStorage.Environment);

                var indexMetadata1 = new IndexTransformerMetadata
                {
                    Id = 1,
                    Etag = 123,
                    ChangeVector = new []
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 123}
                    }
                };

                var indexMetadata2 = new IndexTransformerMetadata
                {
                    Id = 2,
                    Etag = 345,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 345}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1,MetadataStorageType.Index);
                indexMetadataStorage.WriteMetadata(indexMetadata2, MetadataStorageType.Index);

                var fetchedMetadata1 = indexMetadataStorage.ReadMetadata(1, MetadataStorageType.Index);
                var fetchedMetadata2 = indexMetadataStorage.ReadMetadata(2, MetadataStorageType.Index);

                Assert.True(AreEqual(indexMetadata1, fetchedMetadata1));
                Assert.True(AreEqual(indexMetadata2, fetchedMetadata2));
            }
        }

        [Fact]
        public async Task Deleting_index_metadata_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new MetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize(documentDatabase.DocumentsStorage.Environment);

                var indexMetadata1 = new IndexTransformerMetadata
                {
                    Id = 1,
                    Etag = 123,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 123}
                    }
                };

                var indexMetadata2 = new IndexTransformerMetadata
                {
                    Id = 2,
                    Etag = 345,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 345}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1, MetadataStorageType.Index);
                indexMetadataStorage.WriteMetadata(indexMetadata2, MetadataStorageType.Index);

                var resultBeforeDelete = indexMetadataStorage.GetMetadataAfter(0, MetadataStorageType.Index).ToList();
                indexMetadataStorage.DeleteMetadata(1, MetadataStorageType.Index);
                var resultAfterStorage = indexMetadataStorage.GetMetadataAfter(0, MetadataStorageType.Index).ToList();

                Assert.Equal(2, resultBeforeDelete.Count);
                Assert.Equal(1, resultAfterStorage.Count);
            }
        }

        [Fact]
        public async Task GetIndexMetadata_after_etag_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new MetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize(documentDatabase.DocumentsStorage.Environment);

                var indexMetadata1 = new IndexTransformerMetadata
                {
                    Id = 1,
                    Etag = 1,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 1}
                    }
                };

                var indexMetadata2 = new IndexTransformerMetadata
                {
                    Id = 2,
                    Etag = 2,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 2}
                    }
                };

                var indexMetadata3 = new IndexTransformerMetadata
                {
                    Id = 3,
                    Etag = 3,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 3}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1, MetadataStorageType.Index);
                indexMetadataStorage.WriteMetadata(indexMetadata2, MetadataStorageType.Index);
                indexMetadataStorage.WriteMetadata(indexMetadata3, MetadataStorageType.Index);

                var result1 = indexMetadataStorage.GetMetadataAfter(10, MetadataStorageType.Index);

                var result2 = indexMetadataStorage.GetMetadataAfter(3, MetadataStorageType.Index);

                var result3 = indexMetadataStorage.GetMetadataAfter(2, MetadataStorageType.Index);

                Assert.Equal(0, result1.Count());
                Assert.Equal(1, result2.Count());
                Assert.Equal(2, result3.Count());

            }
        }

        private bool AreEqual(IndexTransformerMetadata metadataA, IndexTransformerMetadata metadataB)
        {
            return metadataA.Etag == metadataB.Etag &&
                   metadataA.Id == metadataB.Id &&
                   metadataA.ChangeVector.SequenceEqual(metadataB.ChangeVector);
        }
    }
}
