using System.Linq;

using Raven.Abstractions.Data;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core;

using Xunit;

namespace FastTests.Server.Documents.Tombstones
{
    public class BasicTombstones : RavenTestBase
    {
        [Fact]
        public void CanCreateAndGetTombstone()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                {
                    PutResult result;
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                        {
                            ["Name"] = "John",
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.RavenEntityName] = "Users"
                            }
                        }))
                        {
                            result = database.DocumentsStorage.Put(context, "key/1", null, doc);
                        }

                        tx.Commit();
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        Assert.True(database.DocumentsStorage.Delete(context, "key/1", null));

                        tx.Commit();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var tombstones = database
                            .DocumentsStorage
                            .GetTombstonesAfter(context, "Users", 0, 0, int.MaxValue)
                            .ToList();

                        Assert.Equal(1, tombstones.Count);

                        var tombstone = tombstones[0];

                        Assert.True(tombstone.StorageId > 0);
                        Assert.Equal(result.ETag, tombstone.DeletedEtag);
                        Assert.Equal(result.ETag + 1, tombstone.Etag);
                        Assert.Equal(result.Key, tombstone.Key);
                    }
                }
            }
        }

        private static BlittableJsonReaderObject CreateDocument(MemoryOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        private static DocumentDatabase CreateDocumentDatabase()
        {
            var documentDatabase = new DocumentDatabase("Test", new RavenConfiguration { Core = { RunInMemory = true } });
            documentDatabase.Initialize();

            return documentDatabase;
        }
    }
}