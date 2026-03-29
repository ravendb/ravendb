using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Attachments;
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

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
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
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Verify it exists
            using (var session = store.OpenSession())
            {
                Assert.NotNull(session.Load<dynamic>("Orders/1"));
            }

            // Now delete it
            var deleteOps = new List<CdcSinkDocumentOp> { CreateDeleteOp("Orders/1") };
            var deleteCmd = new CdcSinkBatchCommand(database, deleteOps, "test-config", null, null, null, null, null, null);
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
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null, null);
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
            var embCmd = new CdcSinkBatchCommand(database, embOps, "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
                "test-config", null, null, null, null, null, null);
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
            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-config", null, null, null, null, null, null);
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
            var batchCmd = new CdcSinkBatchCommand(database, batchOps, "test-config", null, null, null, null, null, null);
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

            var tableConfig = CreateRootTableConfig(patch: "this.ComputedField = $row.extra_info + ' processed';");
            var processor = CreateRootProcessor(tableConfig);

            // Build a CdcSinkDocumentProcessor to get the CombinedPatchRequest
            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);

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

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
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

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
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

        [Fact]
        public async Task BinaryToAttachment()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var fileContent = new byte[] { 0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34 }; // %PDF-1.4

            var config = new CdcSinkTableConfig
            {
                Name = "Documents",
                SourceTableSchema = "public",
                SourceTableName = "documents",
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "id", "Id" },
                    { "name", "Name" }
                },
                AttachmentNameMapping = new Dictionary<string, string>
                {
                    { "content", "FileContent" }
                },
                PrimaryKeyColumns = new List<string> { "id" }
            };

            var processor = new CdcSinkTableProcessor
            {
                RootConfig = config,
                CollectionName = "Documents",
                IsRoot = true
            };

            var mappedData = new DynamicJsonValue
            {
                ["Id"] = 1,
                ["Name"] = "doc.pdf",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Documents"
                }
            };

            var rawData = new Dictionary<string, object>
            {
                { "id", 1 },
                { "name", "doc.pdf" },
                { "content", fileContent }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Documents/1", mappedData, rawData, processor)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Documents/1");
                Assert.NotNull(doc);

                // Document should have Id and Name properties
                doc.Data.TryGet("Id", out long id);
                Assert.Equal(1L, id);
                doc.Data.TryGet("Name", out string name);
                Assert.Equal("doc.pdf", name);

                // Document should NOT have the binary column as a property
                Assert.False(doc.Data.TryGet("FileContent", out object _),
                    "Binary column mapped as attachment should not appear as a document property");

                // Attachment should exist on the document
                var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                    readCtx, "Documents/1", "FileContent", AttachmentType.Document, changeVector: null);
                Assert.NotNull(attachment);
                Assert.Equal("FileContent", attachment.Name);
                Assert.Equal("application/octet-stream", attachment.ContentType);

                // Verify the attachment content matches
                using var memoryStream = new MemoryStream();
                attachment.Stream.CopyTo(memoryStream);
                var storedBytes = memoryStream.ToArray();
                Assert.Equal(fileContent, storedBytes);
            }
        }
        [Fact]
        public async Task PropertyRetention_OnUpdate()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Put initial document with an extra property not in the CDC mapping
            using (var session = store.OpenSession())
            {
                session.Store(new { OrderId = 1, CustomerName = "Alice", ExtraField = "keep me" }, "Orders/1");
                session.Advanced.GetMetadataFor(session.Load<dynamic>("Orders/1"))[Constants.Documents.Metadata.Collection] = "Orders";
                session.SaveChanges();
            }

            // CDC Put arrives with only OrderId and CustomerName mapped
            var mappedData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Bob",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Orders/1", mappedData)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                // Updated property should reflect the new value
                doc.Data.TryGet("CustomerName", out string customerName);
                Assert.Equal("Bob", customerName);

                // ExtraField should still be present (Object.assign retains existing properties)
                doc.Data.TryGet("ExtraField", out string extraField);
                Assert.Equal("keep me", extraField);
            }
        }

        [Fact]
        public async Task EmbeddedUpdate_Array_RetainsExistingProperties()
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
                "test-config", null, null, null, null, null, null);
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

            // Insert initial item with an extra property (ExtraInfo)
            var insertData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "Widget",
                ["Quantity"] = 5,
                ["ExtraInfo"] = "retain this"
            };
            var insertCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(insertCmd);

            // Update the same item but only send Product and LineId (not ExtraInfo)
            var updateData = new DynamicJsonValue
            {
                ["LineId"] = 10,
                ["Product"] = "SuperWidget"
            };
            var updateCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", updateData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
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

                // ExtraInfo should be retained from the original insert
                item.TryGet("ExtraInfo", out string extraInfo);
                Assert.Equal("retain this", extraInfo);

                // Quantity should also be retained (it was not in the update)
                item.TryGet("Quantity", out long quantity);
                Assert.Equal(5L, quantity);
            }
        }

        [Fact]
        public async Task EmbeddedUpdate_Map_RetainsExistingProperties()
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
                "test-config", null, null, null, null, null, null);
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

            // Insert initial entry with an extra property
            var insertData = new DynamicJsonValue
            {
                ["Key"] = "color",
                ["Value"] = "red",
                ["Source"] = "user-input"
            };
            var insertCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(insertCmd);

            // Update the same key but only send Key and Value (not Source)
            var updateData = new DynamicJsonValue
            {
                ["Key"] = "color",
                ["Value"] = "blue"
            };
            var updateCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", updateData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(updateCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Attributes", out BlittableJsonReaderObject attributes);
                Assert.NotNull(attributes);
                attributes.TryGet("color", out BlittableJsonReaderObject colorEntry);
                Assert.NotNull(colorEntry);

                colorEntry.TryGet("Value", out string value);
                Assert.Equal("blue", value);

                // Source should be retained from the original insert
                colorEntry.TryGet("Source", out string source);
                Assert.Equal("user-input", source);
            }
        }

        [Fact]
        public async Task EmbeddedUpdate_Value_RetainsExistingProperties()
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
                "test-config", null, null, null, null, null, null);
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

            // Insert initial value with an extra property
            var insertData = new DynamicJsonValue
            {
                ["Carrier"] = "FedEx",
                ["TrackingNumber"] = "ABC123",
                ["EstimatedDate"] = "2026-04-01"
            };
            var insertCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(insertCmd);

            // Update: only send Carrier (not EstimatedDate or TrackingNumber)
            var updateData = new DynamicJsonValue
            {
                ["Carrier"] = "UPS"
            };
            var updateCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp>
                {
                    CreateEmbeddedOp("Orders/1", updateData, CdcSinkOperation.Upsert, embeddedProcessor)
                },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(updateCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("ShippingInfo", out BlittableJsonReaderObject shippingInfo);
                Assert.NotNull(shippingInfo);

                shippingInfo.TryGet("Carrier", out string carrier);
                Assert.Equal("UPS", carrier);

                // These should be retained from the original insert
                shippingInfo.TryGet("TrackingNumber", out string trackingNumber);
                Assert.Equal("ABC123", trackingNumber);

                shippingInfo.TryGet("EstimatedDate", out string estimatedDate);
                Assert.Equal("2026-04-01", estimatedDate);
            }
        }

        [Fact]
        public async Task SequentialPutDeletePut_LastPutWins()
        {
            // Simulates CDC sequence: put, delete, put — the last put should win
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var put1 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                ["Amount"] = 50.0,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var delete = CreateDeleteOp("Orders/1");

            var put2 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Bob",
                ["Amount"] = 75.0,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var ops = new List<CdcSinkDocumentOp> { put1, delete, put2 };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);
                doc.Data.TryGet("CustomerName", out string name);
                Assert.Equal("Bob", name);
                doc.Data.TryGet("Amount", out double amount);
                Assert.Equal(75.0, amount);
            }
        }

        [Fact]
        public async Task SequentialPutDeleteOnly_DocumentIsDeleted()
        {
            // Simulates CDC sequence: put, delete — document should end up deleted
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Pre-create the document
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["OrderId"] = 1,
                    ["CustomerName"] = "Alice",
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Orders"
                    }
                };
                using var blittable = context.ReadObject(djv, "Orders/1");
                database.DocumentsStorage.Put(context, "Orders/1", null, blittable);
                tx.Commit();
            }

            var put = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Bob",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });
            var delete = CreateDeleteOp("Orders/1");

            var ops = new List<CdcSinkDocumentOp> { put, delete };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.Null(doc);
            }
        }

        [Fact]
        public async Task DeleteThenEmbed_CreatesStubWithEmbed()
        {
            // Simulates CDC sequence: delete, embed — should create a stub document with the embed applied
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Pre-create document
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["OrderId"] = 1,
                    ["CustomerName"] = "Alice",
                    ["Lines"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["LineId"] = 1L, ["Product"] = "OldProduct" }
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Orders"
                    }
                };
                using var blittable = context.ReadObject(djv, "Orders/1");
                database.DocumentsStorage.Put(context, "Orders/1", null, blittable);
                tx.Commit();
            }

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                Type = CdcSinkRelationType.Array,
                JoinColumns = new List<string> { "order_id" },
                PrimaryKeyColumns = new List<string> { "line_id" },
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" }
                }
            };
            var embProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var delete = CreateDeleteOp("Orders/1");
            var embed = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 99L,
                ["Product"] = "NewProduct"
            }, CdcSinkOperation.Upsert, embProcessor);

            var ops = new List<CdcSinkDocumentOp> { delete, embed };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);

                // Old data is gone (delete cleared it), but new embed is applied on a stub
                doc.Data.TryGet("CustomerName", out string name);
                Assert.Null(name);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);

                var line = lines[0] as BlittableJsonReaderObject;
                Assert.NotNull(line);
                line.TryGet("Product", out string product);
                Assert.Equal("NewProduct", product);
            }
        }

        [Fact]
        public async Task PutDeletePutEmbed_FinalStateHasLastPutAndEmbed()
        {
            // Full sequence: put, delete, put, embed — last put creates fresh doc, embed adds to it
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                Type = CdcSinkRelationType.Array,
                JoinColumns = new List<string> { "order_id" },
                PrimaryKeyColumns = new List<string> { "line_id" },
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" }
                }
            };
            var embProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var put1 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var delete = CreateDeleteOp("Orders/1");

            var put2 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Charlie",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var embed = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 1L,
                ["Product"] = "Widget"
            }, CdcSinkOperation.Upsert, embProcessor);

            var ops = new List<CdcSinkDocumentOp> { put1, delete, put2, embed };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("CustomerName", out string name);
                Assert.Equal("Charlie", name);

                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);

                var line = lines[0] as BlittableJsonReaderObject;
                line.TryGet("Product", out string product);
                Assert.Equal("Widget", product);
            }
        }

        [Fact]
        public async Task DeleteClearsEmbedsBefore()
        {
            // Sequence: embed, embed, delete, embed — only the last embed survives
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Pre-create document
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["OrderId"] = 1,
                    ["CustomerName"] = "Alice",
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Orders"
                    }
                };
                using var blittable = context.ReadObject(djv, "Orders/1");
                database.DocumentsStorage.Put(context, "Orders/1", null, blittable);
                tx.Commit();
            }

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                Type = CdcSinkRelationType.Array,
                JoinColumns = new List<string> { "order_id" },
                PrimaryKeyColumns = new List<string> { "line_id" },
                ColumnsMapping = new Dictionary<string, string>
                {
                    { "line_id", "LineId" },
                    { "product", "Product" }
                }
            };
            var embProcessor = CreateEmbeddedProcessor(embeddedConfig);

            var embed1 = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 1L,
                ["Product"] = "Apples"
            }, CdcSinkOperation.Upsert, embProcessor);

            var embed2 = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 2L,
                ["Product"] = "Bananas"
            }, CdcSinkOperation.Upsert, embProcessor);

            var delete = CreateDeleteOp("Orders/1");

            var embed3 = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 3L,
                ["Product"] = "Cherries"
            }, CdcSinkOperation.Upsert, embProcessor);

            var ops = new List<CdcSinkDocumentOp> { embed1, embed2, delete, embed3 };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);

                // Only the embed after the delete survives
                doc.Data.TryGet("Lines", out BlittableJsonReaderArray lines);
                Assert.NotNull(lines);
                Assert.Equal(1, lines.Length);

                var line = lines[0] as BlittableJsonReaderObject;
                line.TryGet("Product", out string product);
                Assert.Equal("Cherries", product);
            }
        }

        [Fact]
        public async Task MultiplePutsAccumulate_ObjectAssign()
        {
            // Two puts on the same doc: second put adds new fields, retains first put's fields
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var put1 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var put2 = CreatePutOp("Orders/1", new DynamicJsonValue
            {
                ["Amount"] = 99.5,
                ["Status"] = "Confirmed",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders"
                }
            });

            var ops = new List<CdcSinkDocumentOp> { put1, put2 };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                command.Execute(context, null);
                tx.Commit();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);

                // First put's fields retained
                doc.Data.TryGet("OrderId", out long orderId);
                Assert.Equal(1, orderId);
                doc.Data.TryGet("CustomerName", out string name);
                Assert.Equal("Alice", name);

                // Second put's fields added
                doc.Data.TryGet("Amount", out double amount);
                Assert.Equal(99.5, amount);
                doc.Data.TryGet("Status", out string status);
                Assert.Equal("Confirmed", status);
            }
        }
    }
}
