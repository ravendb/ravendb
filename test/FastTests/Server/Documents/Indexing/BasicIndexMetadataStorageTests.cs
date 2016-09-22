using System;
using System.Collections.Generic;
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

        private bool AreEqual(IndexMetadata metadataA, IndexMetadata metadataB)
        {
            return metadataA.Etag == metadataB.Etag &&
                   metadataA.IndexId == metadataB.IndexId &&
                   metadataA.ChangeVector.SequenceEqual(metadataB.ChangeVector);
        }
    }
}
