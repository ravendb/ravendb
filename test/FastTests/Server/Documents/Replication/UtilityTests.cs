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
    public class UtilityTests : RavenTestBase
    {
        [Fact]
        public async Task Receive_replication_documents_should_work()
        {
            using (await GetDocumentStore(modifyDatabaseDocument: document => document.Id = "TestDB2"))
            using (var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("TestDB2"))
            {
                DocumentsOperationContext context;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {

                }
            }
        }

        [Fact]
        public async Task Extract_change_vector_from_document_metadata_should_work()
        {
            using (await GetDocumentStore(modifyDatabaseDocument:document => document.Id = "TestDB"))
            using (var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("TestDB"))
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
                            ["Key"] = new DynamicJsonArray(guid1.ToByteArray().Cast<object>()),
                            ["Value"] = 1L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = new DynamicJsonArray(guid2.ToByteArray().Cast<object>()),
                            ["Value"] = 3L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = new DynamicJsonArray(guid3.ToByteArray().Cast<object>()),
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

                    Assert.Equal(changeVector[0].Item1.Select(Convert.ToByte), guid1.ToByteArray());
                    Assert.Equal(changeVector[0].Item2, 1L);

                    Assert.Equal(changeVector[1].Item1.Select(Convert.ToByte), guid2.ToByteArray());
                    Assert.Equal(changeVector[1].Item2, 3L);

                    Assert.Equal(changeVector[2].Item1.Select(Convert.ToByte), guid3.ToByteArray());
                    Assert.Equal(changeVector[2].Item2, 5L);
                }
            }
        }
    }
}
