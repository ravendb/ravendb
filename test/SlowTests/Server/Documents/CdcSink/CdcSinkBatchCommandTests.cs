using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Documents.CdcSink.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkBatchCommandTests : RavenTestBase
    {
        public CdcSinkBatchCommandTests(ITestOutputHelper output) : base(output)
        {
        }

        private static CdcSinkTableConfig CreateRootTableConfig(string collectionName = "Orders", string patch = null)
        {
            return new CdcSinkTableConfig
            {
                Name = collectionName,
                SourceTableSchema = "public",
                SourceTableName = "orders",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "order_id", "OrderId" },
                    { "customer_name", "CustomerName" },
                    { "amount", "Amount" }
                },
                PrimaryKeyColumns = new List<string> { "order_id" },
                Patch = patch
            };
        }

        private static CdcSinkTableProcessor CreateRootProcessor(CdcSinkTableConfig config = null, string collectionName = "Orders")
        {
            config ??= CreateRootTableConfig(collectionName);
            return new CdcSinkTableProcessor
            {
                RootConfig = config,
                CollectionName = collectionName,
                IsRoot = true
            };
        }

        private static CdcSinkTableProcessor CreateEmbeddedProcessor(
            CdcSinkEmbeddedTableConfig embeddedConfig,
            string collectionName = "Orders",
            CdcSinkTableConfig rootConfig = null)
        {
            rootConfig ??= CreateRootTableConfig(collectionName);
            return new CdcSinkTableProcessor
            {
                RootConfig = rootConfig,
                CollectionName = collectionName,
                IsRoot = false,
                EmbeddedConfig = embeddedConfig,
                PathFromRoot = new List<EmbeddedPathSegment>
                {
                    new EmbeddedPathSegment { Config = embeddedConfig }
                },
                RootJoinColumns = new List<string> { "order_id" }
            };
        }

        private static CdcSinkDocumentOp CreatePutOp(string documentId, DynamicJsonValue mappedData,
            Dictionary<string, object> rawData = null, CdcSinkTableProcessor processor = null)
        {
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Put,
                DocumentId = documentId,
                Processor = processor ?? CreateRootProcessor(),
                MappedData = mappedData,
                RawData = rawData ?? new Dictionary<string, object>(),
                Operation = CdcSinkOperation.Upsert
            };
        }

        private static CdcSinkDocumentOp CreateDeleteOp(string documentId, CdcSinkTableProcessor processor = null)
        {
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Delete,
                DocumentId = documentId,
                Processor = processor ?? CreateRootProcessor(),
                MappedData = new DynamicJsonValue(),
                RawData = new Dictionary<string, object>(),
                Operation = CdcSinkOperation.Delete
            };
        }

        private static CdcSinkDocumentOp CreateEmbeddedOp(
            string documentId,
            DynamicJsonValue mappedData,
            CdcSinkOperation operation,
            CdcSinkTableProcessor embeddedProcessor,
            Dictionary<string, object> rawData = null)
        {
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.EmbeddedModify,
                DocumentId = documentId,
                Processor = embeddedProcessor,
                MappedData = mappedData,
                RawData = rawData ?? new Dictionary<string, object>(),
                Operation = operation
            };
        }

        [Fact]
        public async Task PutRootDocument()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var mappedData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                ["Amount"] = 99.5,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Orders/1", mappedData)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (var session = store.OpenSession())
            {
                var doc = session.Load<dynamic>("Orders/1");
                Assert.NotNull(doc);
                Assert.Equal(1L, (long)doc.OrderId);
                Assert.Equal("Alice", (string)doc.CustomerName);
                Assert.Equal(99.5, (double)doc.Amount);

                var metadata = session.Advanced.GetMetadataFor(doc);
                Assert.Equal("Orders", metadata[Constants.Documents.Metadata.Collection]);
            }
        }

        [Fact]
        public async Task DeleteRootDocument()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // First, put a document
            var mappedData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };

            var putOps = new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", mappedData) };
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Verify it exists
            using (var session = store.OpenSession())
            {
                Assert.NotNull(session.Load<dynamic>("Orders/1"));
            }

            // Now delete it
            var deleteOps = new List<CdcSinkDocumentOp> { CreateDeleteOp("Orders/1") };
            var deleteCmd = new CdcSinkBatchCommand(database, deleteOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(deleteCmd);

            using (var session = store.OpenSession())
            {
                Assert.Null(session.Load<dynamic>("Orders/1"));
            }
        }

        [Fact]
        public async Task EmbeddedUpsert_Array()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // First, put the parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putOps = new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) };
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Create embedded array config
            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" },
                    { "qty", "Quantity" }
                },
                PrimaryKeyColumns = new List<string> { "line_id" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Array
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var itemData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget",
                ["Quantity"] = 5
            };

            var embOps = new List<CdcSinkDocumentOp>
            {
                CreateEmbeddedOp("Orders/1", itemData, CdcSinkOperation.Upsert, embeddedProcessor)
            };
            var embCmd = new CdcSinkBatchCommand(database, embOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(embCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);

                var item = (BlittableJsonReaderObject)lines[0];
                item.TryGet("LineId", out long lineId);
                Assert.Equal(10L, lineId);
                item.TryGet("Product", out string product);
                Assert.Equal("Widget", product);
                item.TryGet("Quantity", out long quantity);
                Assert.Equal(5L, quantity);
            }
        }

        [Fact]
        public async Task EmbeddedUpdate_Array()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" },
                    { "qty", "Quantity" }
                },
                PrimaryKeyColumns = new List<string> { "line_id" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Array
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            // Insert initial item
            var insertData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget",
                ["Quantity"] = 5
            };
            var insertCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(insertCmd);

            // Update the same item (same PK)
            var updateData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "SuperWidget",
                ["Quantity"] = 20
            };
            var updateCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", updateData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(updateCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);

                var item = (BlittableJsonReaderObject)lines[0];
                item.TryGet("Product", out string product);
                Assert.Equal("SuperWidget", product);
                item.TryGet("Quantity", out long quantity);
                Assert.Equal(20L, quantity);
            }
        }

        [Fact]
        public async Task EmbeddedDelete_Array()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" }
                },
                PrimaryKeyColumns = new List<string> { "line_id" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Array
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            // Insert item
            var insertData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget"
            };
            var insertCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(insertCmd);

            // Verify it was inserted
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext verifyCtx))
            using (verifyCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(verifyCtx, "Orders/1");
                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);
            }

            // Delete the item
            var deleteData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget"
            };
            var deleteCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", deleteData, CdcSinkOperation.Delete, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(deleteCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(0, lines.Length);
            }
        }

        [Fact]
        public async Task EmbeddedUpsert_Map()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "order_attributes",
                PropertyName = "Attributes",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "attr_key", "Key" },
                    { "attr_value", "Value" }
                },
                PrimaryKeyColumns = new List<string> { "attr_key" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Map
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var itemData = new DynamicJsonValue
            {
                ["Key"] = "color",
                ["Value"] = "red"
            };
            var embCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", itemData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(embCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                // Map key is built from PK column mapped value: "color"
                doc.Data.TryGet("Attributes", out BlittableJsonReaderObject attributes);
                Assert.NotNull(attributes);
                attributes.TryGet("color", out BlittableJsonReaderObject colorEntry);
                Assert.NotNull(colorEntry);
                colorEntry.TryGet("Value", out string value);
                Assert.Equal("red", value);
            }
        }

        [Fact]
        public async Task EmbeddedUpsert_Value()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "shipping_info",
                PropertyName = "ShippingInfo",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "carrier", "Carrier" },
                    { "tracking_no", "TrackingNumber" }
                },
                PrimaryKeyColumns = new List<string> { "order_id" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Value
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var itemData = new DynamicJsonValue
            {
                ["Carrier"] = "FedEx",
                ["TrackingNumber"] = "ABC123"
            };
            var embCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", itemData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(embCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("ShippingInfo", out BlittableJsonReaderObject shippingInfo);
                Assert.NotNull(shippingInfo);
                shippingInfo.TryGet("Carrier", out string carrier);
                Assert.Equal("FedEx", carrier);
                shippingInfo.TryGet("TrackingNumber", out string trackingNumber);
                Assert.Equal("ABC123", trackingNumber);
            }
        }

        [Fact]
        public async Task BatchCoalescing()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put parent document
            var parentData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };
            var putOps = new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) };
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" }
                },
                PrimaryKeyColumns = new List<string> { "line_id" },
                JoinColumns = new List<string> { "order_id" },
                Type = CdcSinkRelationType.Array
            };

            var embeddedProcessor = CreateEmbeddedProcessor(embeddedConfig);

            // Two embedded ops for the same parent document in a single batch
            var item1 = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget"
            };
            var item2 = new DynamicJsonValue
            {
                ["LineId"] = 20,
                ["Product"] = "Gadget"
            };

            var batchOps = new List<CdcSinkDocumentOp>
            {
                CreateEmbeddedOp("Orders/1", item1, CdcSinkOperation.Upsert, embeddedProcessor),
                CreateEmbeddedOp("Orders/1", item2, CdcSinkOperation.Upsert, embeddedProcessor)
            };
            var batchCmd = new CdcSinkBatchCommand(database, batchOps, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(batchCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(2, lines.Length);

                var products = new List<string>();
                for (int i = 0; i < lines.Length; i++)
                {
                    var item = (BlittableJsonReaderObject)lines[i];
                    item.TryGet("Product", out string p);
                    products.Add(p);
                }
                products.Sort();
                Assert.Equal("Gadget", products[0]);
                Assert.Equal("Widget", products[1]);
            }
        }

        [Fact]
        public async Task PatchWithDollarRow()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var config = CreateRootTableConfig(patch: "this.ComputedField = args['$row'].extra_info + ' processed';");
            var processor = CreateRootProcessor(config);

            var mappedData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };

            var rawData = new Dictionary<string, object>
            {
                { "order_id", 1 },
                { "customer_name", "Alice" },
                { "extra_info", "rush" }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Orders/1", mappedData, rawData, processor)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);
                doc.Data.TryGet("ComputedField", out string computedField);
                Assert.Equal("rush processed", computedField);
            }
        }

        [Fact]
        public async Task PatchError_AbortsSingleDocument()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // First op: valid document with no patch
            var goodProcessor = CreateRootProcessor();
            var goodData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };

            // Second op: document with an invalid patch that will throw
            var badConfig = CreateRootTableConfig("Products", patch: "throw new Error('intentional failure');");
            var badProcessor = new CdcSinkTableProcessor
            {
                RootConfig = badConfig,
                CollectionName = "Products",
                IsRoot = true
            };
            var badData = new DynamicJsonValue
            {
                ["ProductId"] = 99,
                ["Name"] = "BadProduct",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Products"
                }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Orders/1", goodData, processor: goodProcessor),
                CreatePutOp("Products/99", badData, processor: badProcessor)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (var session = store.OpenSession())
            {
                // The good document should have been saved
                var goodDoc = session.Load<dynamic>("Orders/1");
                Assert.NotNull(goodDoc);
                Assert.Equal("Alice", (string)goodDoc.CustomerName);

                // The bad document should NOT have been saved
                var badDoc = session.Load<dynamic>("Products/99");
                Assert.Null(badDoc);
            }
        }
    }
}
