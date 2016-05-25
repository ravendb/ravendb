using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Raven.Server;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationUtilityTests : RavenTestBase
    {
        public readonly string DbName = $"TestDB{Guid.NewGuid()}";

        [Fact(Skip = "Not everything is done, WIP")]
        public async Task Extract_change_vector_from_document_metadata_should_work()
        {
            using (await GetDocumentStore(modifyDatabaseDocument:document => document.Id = DbName))
            using (var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(DbName))
            {
                DocumentsOperationContext context;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var guid1 = Guid.NewGuid();
                    var guid2 = Guid.NewGuid();
                    var guid3 = Guid.NewGuid();
                    var collection = new[]
                    {
                        new DynamicJsonValue
                        {
                            ["Key"] = guid1.ToString(),
                            ["Value"] = 1L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = guid2.ToString(),
                            ["Value"] = 3L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = guid3.ToString(),
                            ["Value"] = 5L
                        },
                    };

                    var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Foo"] = "Bar",
                        [Constants.Metadata] = new DynamicJsonValue
                        {
                            [Constants.DocumentReplication.DocumentChangeVector] = new DynamicJsonArray(collection)
                        }
                    }, "foo/bar");

                    var changeVector = doc.EnumerateChangeVector().ToList();

                    Assert.Equal(changeVector[0].DbId, guid1);
                    Assert.Equal(changeVector[0].Etag, 1L);

                    Assert.Equal(changeVector[1].DbId, guid2);
                    Assert.Equal(changeVector[1].Etag, 3L);

                    Assert.Equal(changeVector[2].DbId, guid3);
                    Assert.Equal(changeVector[2].Etag, 5L);
                }
            }
        }
    }
}
