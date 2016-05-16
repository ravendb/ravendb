using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class UtilityTests : RavenTestBase
    {
        [Fact]
        public async Task Extract_change_vector_from_document_metadata_should_work()
        {
            using (await GetDocumentStore(modifyDatabaseDocument:document => document.Id = "TestDB"))
            using (var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("TestDB"))
            {
                DocumentsOperationContext context;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var collection = new[]
                    {
                        new DynamicJsonValue
                        {
                            ["Key"] = new DynamicJsonArray(Guid.NewGuid().ToByteArray().Cast<object>()),
                            ["Value"] = 1L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = new DynamicJsonArray(Guid.NewGuid().ToByteArray().Cast<object>()),
                            ["Value"] = 3L
                        },
                        new DynamicJsonValue
                        {
                            ["Key"] = new DynamicJsonArray(Guid.NewGuid().ToByteArray().Cast<object>()),
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
                }
            }
        }
    }
}
