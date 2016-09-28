using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicIndexMetadataStorageTests : RavenTestBase
    {
        [Fact]
        public async Task Read_and_write_index_metadata_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new IndexMetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize();

                var indexMetadata1 = new IndexMetadata
                {
                    IndexId = 1,
                    Etag = 123,
                    ChangeVector = new []
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 123}
                    }
                };

                var indexMetadata2 = new IndexMetadata
                {
                    IndexId = 2,
                    Etag = 345,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 345}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1);
                indexMetadataStorage.WriteMetadata(indexMetadata2);

                var fetchedMetadata1 = indexMetadataStorage.ReadMetadata(1);
                var fetchedMetadata2 = indexMetadataStorage.ReadMetadata(2);

                Assert.True(AreEqual(indexMetadata1, fetchedMetadata1));
                Assert.True(AreEqual(indexMetadata2, fetchedMetadata2));
            }
        }

        [Fact]
        public async Task Deleting_index_metadata_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new IndexMetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize();

                var indexMetadata1 = new IndexMetadata
                {
                    IndexId = 1,
                    Etag = 123,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 123}
                    }
                };

                var indexMetadata2 = new IndexMetadata
                {
                    IndexId = 2,
                    Etag = 345,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 345}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1);
                indexMetadataStorage.WriteMetadata(indexMetadata2);

                var resultBeforeDelete = indexMetadataStorage.GetIndexMetadataAfter(0).ToList();
                indexMetadataStorage.DeleteMetadata(1);
                var resultAfterStorage = indexMetadataStorage.GetIndexMetadataAfter(0).ToList();

                Assert.Equal(2, resultBeforeDelete.Count);
                Assert.Equal(1, resultAfterStorage.Count);
            }
        }

        [Fact]
        public async Task GetIndexMetadata_after_etag_should_work()
        {
            using (var store = GetDocumentStore())
            using (var documentDatabase = await GetDatabase(store.DefaultDatabase))
            using (var indexMetadataStorage = new IndexMetadataStorage(documentDatabase))
            {
                indexMetadataStorage.Initialize();

                var indexMetadata1 = new IndexMetadata
                {
                    IndexId = 1,
                    Etag = 1,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 1}
                    }
                };

                var indexMetadata2 = new IndexMetadata
                {
                    IndexId = 2,
                    Etag = 2,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 2}
                    }
                };

                var indexMetadata3 = new IndexMetadata
                {
                    IndexId = 3,
                    Etag = 3,
                    ChangeVector = new[]
                    {
                        new ChangeVectorEntry {DbId = Guid.NewGuid(), Etag = 3}
                    }
                };

                indexMetadataStorage.WriteMetadata(indexMetadata1);
                indexMetadataStorage.WriteMetadata(indexMetadata2);
                indexMetadataStorage.WriteMetadata(indexMetadata3);

                var result1 = indexMetadataStorage.GetIndexMetadataAfter(10);

                var result2 = indexMetadataStorage.GetIndexMetadataAfter(3);

                var result3 = indexMetadataStorage.GetIndexMetadataAfter(2);

                Assert.Equal(0, result1.Count());
                Assert.Equal(1, result2.Count());
                Assert.Equal(2, result3.Count());

            }
        }

        private bool AreEqual(IndexMetadata metadataA, IndexMetadata metadataB)
        {
            return metadataA.Etag == metadataB.Etag &&
                   metadataA.IndexId == metadataB.IndexId &&
                   metadataA.ChangeVector.SequenceEqual(metadataB.ChangeVector);
        }
    }
}
