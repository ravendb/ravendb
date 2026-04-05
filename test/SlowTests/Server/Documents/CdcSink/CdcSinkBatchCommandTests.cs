using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.Config;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Documents.CdcSink.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkBatchCommandTests : RavenTestBase
    {
        public CdcSinkBatchCommandTests(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// Verifies that the actual document contains all properties from the expected JSON.
        /// Ignores @metadata and extra properties in the actual document.
        /// </summary>
        private static void AssertDocumentMatches(DocumentsOperationContext context, BlittableJsonReaderObject actual, string expectedJson)
        {
            using var expected = context.Sync.ReadForMemory(expectedJson, "expected");
            AssertBlittableMatches(actual, expected, "");
        }

        private static void AssertBlittableMatches(BlittableJsonReaderObject actual, BlittableJsonReaderObject expected, string path)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < expected.Count; i++)
            {
                expected.GetPropertyByIndex(i, ref prop);
                if (prop.Name == Constants.Documents.Metadata.Key)
                    continue;

                var fullPath = string.IsNullOrEmpty(path) ? prop.Name : $"{path}.{prop.Name}";
                Assert.True(actual.TryGetMember(prop.Name, out var actualValue),
                    $"Missing property '{fullPath}'");

                switch (prop.Value)
                {
                    case BlittableJsonReaderObject expectedObj:
                        Assert.IsType<BlittableJsonReaderObject>(actualValue);
                        AssertBlittableMatches((BlittableJsonReaderObject)actualValue, expectedObj, fullPath);
                        break;
                    case BlittableJsonReaderArray expectedArr:
                        var actualArr = Assert.IsType<BlittableJsonReaderArray>(actualValue);
                        Assert.Equal(expectedArr.Length, actualArr.Length);
                        for (int j = 0; j < expectedArr.Length; j++)
                        {
                            if (expectedArr[j] is BlittableJsonReaderObject expItem && actualArr[j] is BlittableJsonReaderObject actItem)
                                AssertBlittableMatches(actItem, expItem, $"{fullPath}[{j}]");
                            else
                                Assert.Equal(expectedArr[j]?.ToString(), actualArr[j]?.ToString());
                        }
                        break;
                    default:
                        Assert.Equal(prop.Value?.ToString(), actualValue?.ToString());
                        break;
                }
            }
        }

        private static CdcSinkTableConfig CreateRootTableConfig(string collectionName = "Orders", string patch = null)
        {
            return new CdcSinkTableConfig
            {
                CollectionName = collectionName,
                SourceTableSchema = "public",
                SourceTableName = "orders",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                    new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" },
                    new CdcColumnMapping { Column = "amount", Name = "Amount" }
                },
                PrimaryKeyColumns = new List<string> { "order_id" },
                Patch = patch
            };
        }

        private static CdcSinkTableProcessor CreateRootProcessor(CdcSinkTableConfig config = null, string collectionName = "Orders")
        {
            config ??= CreateRootTableConfig(collectionName);
            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { config }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor(config.SourceTableSchema ?? "public", config.SourceTableName);
            SetSourceColumnNamesFromConfig(processor, config.Columns);
            return processor;
        }

        private static CdcSinkTableProcessor CreateEmbeddedProcessor(
            CdcSinkEmbeddedTableConfig embeddedConfig,
            string collectionName = "Orders",
            CdcSinkTableConfig rootConfig = null)
        {
            rootConfig ??= CreateRootTableConfig(collectionName);
            rootConfig.EmbeddedTables = new List<CdcSinkEmbeddedTableConfig> { embeddedConfig };
            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { rootConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);

            // Set column names on root processor too
            var rootProcessor = docProcessor.GetProcessor(rootConfig.SourceTableSchema ?? "public", rootConfig.SourceTableName);
            SetSourceColumnNamesFromConfig(rootProcessor, rootConfig.Columns);

            var processor = docProcessor.GetProcessor(embeddedConfig.SourceTableSchema ?? "", embeddedConfig.SourceTableName);
            SetSourceColumnNamesFromConfig(processor, embeddedConfig.Columns);
            return processor;
        }

        /// <summary>
        /// Sets SourceColumnNames from the processor's configuration.
        /// In production, providers set these from DB schema metadata; in tests we derive
        /// them from config by collecting all referenced column names (mappings, PKs, joins).
        /// </summary>
        private static void SetSourceColumnNamesFromConfig(CdcSinkTableProcessor processor, List<CdcColumnMapping> columns)
        {
            var nameSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var nameList = new List<string>();

            void Add(string name)
            {
                if (nameSet.Add(name))
                    nameList.Add(name);
            }

            // Mapped columns
            for (int i = 0; i < columns.Count; i++)
                Add(columns[i].Column);

            // PK columns
            var pkColumns = processor.IsRoot ? processor.RootConfig.PrimaryKeyColumns : processor.EmbeddedConfig?.PrimaryKeyColumns;
            if (pkColumns != null)
                foreach (var pk in pkColumns)
                    Add(pk);

            // Join columns (embedded → root)
            if (processor.RootJoinColumns != null)
                foreach (var jc in processor.RootJoinColumns)
                    Add(jc);

            // Linked table join columns
            if (processor.LinkedTables != null)
                foreach (var lt in processor.LinkedTables)
                    foreach (var jc in lt.JoinColumns)
                        Add(jc);

            processor.SetSourceColumnNames(nameList.ToArray());
        }

        internal static (string[] Names, object[] Values) DictToValues(Dictionary<string, object> dict)
        {
            var names = new string[dict.Count];
            var values = new object[dict.Count];
            int i = 0;
            foreach (var kvp in dict)
            {
                names[i] = kvp.Key;
                values[i] = kvp.Value;
                i++;
            }
            return (names, values);
        }

        private static object[] ToRawValues(Dictionary<string, object> dict, CdcSinkTableProcessor processor)
        {
            if (dict == null || dict.Count == 0)
                return processor.SourceColumnNames != null ? new object[processor.SourceColumnNames.Length] : Array.Empty<object>();

            // In production, SourceColumnNames comes from the DB schema and includes ALL columns.
            // In tests, the dict may have extra columns not in the config (e.g., unmapped columns
            // accessed via $row in patches). Extend the column names to include them.
            var existingNames = processor.SourceColumnNames;
            var allNames = new List<string>(existingNames ?? Array.Empty<string>());
            foreach (var key in dict.Keys)
            {
                bool found = false;
                for (int i = 0; i < allNames.Count; i++)
                {
                    if (string.Equals(allNames[i], key, StringComparison.OrdinalIgnoreCase))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    allNames.Add(key);
            }

            if (existingNames == null || allNames.Count != existingNames.Length)
                processor.SetSourceColumnNames(allNames.ToArray());

            var columnNames = processor.SourceColumnNames;
            var values = new object[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
                dict.TryGetValue(columnNames[i], out values[i]);
            return values;
        }

        private static CdcSinkDocumentOp CreatePutOp(string documentId, DynamicJsonValue mappedData,
            Dictionary<string, object> rawData = null, CdcSinkTableProcessor processor = null)
        {
            processor ??= CreateRootProcessor();
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Put,
                DocumentId = documentId,
                Processor = processor,
                MappedData = mappedData,
                RawValues = ToRawValues(rawData, processor),
                Operation = CdcSinkOperation.Upsert
            };
        }

        private static CdcSinkDocumentOp CreateDeleteOp(string documentId, CdcSinkTableProcessor processor = null,
            Dictionary<string, object> rawData = null)
        {
            processor ??= CreateRootProcessor();
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Delete,
                DocumentId = documentId,
                Processor = processor,
                MappedData = new DynamicJsonValue(),
                RawValues = ToRawValues(rawData, processor),
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
                RawValues = ToRawValues(rawData, embeddedProcessor),
                Operation = operation
            };
        }

        [RavenFact(RavenTestCategory.Sinks)]
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

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "OrderId": 1,
                        "CustomerName": "Alice",
                        "Amount": 99.5
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" },
                    new CdcColumnMapping { Column = "qty", Name = "Quantity" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Lines": [
                            {
                                "LineId": 10,
                                "Product": "Widget",
                                "Quantity": 5
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" },
                    new CdcColumnMapping { Column = "qty", Name = "Quantity" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Lines": [
                            {
                                "LineId": 10,
                                "Product": "SuperWidget",
                                "Quantity": 20
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
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
                Assert.NotNull(doc);
                AssertDocumentMatches(verifyCtx, doc.Data, """
                    {
                        "Lines": [
                            {
                                "LineId": 10,
                                "Product": "Widget"
                            }
                        ]
                    }
                    """);
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Lines": []
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "attr_key", Name = "Key" },
                    new CdcColumnMapping { Column = "attr_value", Name = "Value" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Attributes": {
                            "color": {
                                "Value": "red"
                            }
                        }
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "carrier", Name = "Carrier" },
                    new CdcColumnMapping { Column = "tracking_no", Name = "TrackingNumber" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "ShippingInfo": {
                            "Carrier": "FedEx",
                            "TrackingNumber": "ABC123"
                        }
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
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

        [RavenFact(RavenTestCategory.Sinks)]
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "ComputedField": "rush processed"
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
            var badProcessor = CreateRootProcessor(badConfig, "Products");
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

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                // The good document should have been saved
                var goodDoc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(goodDoc);
                AssertDocumentMatches(readCtx, goodDoc.Data, """
                    {
                        "CustomerName": "Alice"
                    }
                    """);

                // The bad document should NOT have been saved
                var badDoc = database.DocumentsStorage.Get(readCtx, "Products/99");
                Assert.Null(badDoc);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task BinaryToAttachment()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var fileContent = new byte[] { 0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34 }; // %PDF-1.4

            var config = new CdcSinkTableConfig
            {
                CollectionName = "Documents",
                SourceTableSchema = "public",
                SourceTableName = "documents",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" },
                    new CdcColumnMapping { Column = "content", Name = "FileContent", Type = CdcColumnType.Attachment }
                },
                PrimaryKeyColumns = new List<string> { "id" }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { config }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "documents");

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

                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Id": 1,
                        "Name": "doc.pdf"
                    }
                    """);

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
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task VectorToAttachment_FloatArray()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var embedding = new float[] { 0.1f, 0.2f, 0.3f, 0.4f, 0.5f };

            var config = new CdcSinkTableConfig
            {
                CollectionName = "Products",
                SourceTableSchema = "public",
                SourceTableName = "products",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" },
                    new CdcColumnMapping { Column = "embedding", Name = "vector", Type = CdcColumnType.Attachment }
                },
                PrimaryKeyColumns = new List<string> { "id" }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { config }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "products");

            var mappedData = new DynamicJsonValue
            {
                ["Id"] = 1,
                ["Name"] = "Widget",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Products"
                }
            };

            var rawData = new Dictionary<string, object>
            {
                { "id", 1 },
                { "name", "Widget" },
                { "embedding", embedding }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Products/1", mappedData, rawData, processor)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                    readCtx, "Products/1", "vector", AttachmentType.Document, changeVector: null);
                Assert.NotNull(attachment);

                using var ms = new MemoryStream();
                attachment.Stream.CopyTo(ms);
                var storedBytes = ms.ToArray();

                // Verify the stored bytes are the raw float representation
                Assert.Equal(embedding.Length * sizeof(float), storedBytes.Length);
                var restored = new float[embedding.Length];
                Buffer.BlockCopy(storedBytes, 0, restored, 0, storedBytes.Length);
                for (int i = 0; i < embedding.Length; i++)
                    Assert.Equal(embedding[i], restored[i]);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task TextToAttachment_String()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var config = new CdcSinkTableConfig
            {
                CollectionName = "Articles",
                SourceTableSchema = "public",
                SourceTableName = "articles",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "title", Name = "Title" },
                    new CdcColumnMapping { Column = "body", Name = "content.txt", Type = CdcColumnType.Attachment }
                },
                PrimaryKeyColumns = new List<string> { "id" }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { config }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "articles");

            var mappedData = new DynamicJsonValue
            {
                ["Id"] = 1,
                ["Title"] = "Hello",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Articles"
                }
            };

            var rawData = new Dictionary<string, object>
            {
                { "id", 1 },
                { "title", "Hello" },
                { "body", "This is the full article text." }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Articles/1", mappedData, rawData, processor)
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                    readCtx, "Articles/1", "content.txt", AttachmentType.Document, changeVector: null);
                Assert.NotNull(attachment);
                Assert.Equal("text/plain; charset=utf-8", attachment.ContentType);

                using var ms = new MemoryStream();
                attachment.Stream.CopyTo(ms);
                var text = Encoding.UTF8.GetString(ms.ToArray());
                Assert.Equal("This is the full article text.", text);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "CustomerName": "Bob",
                        "ExtraField": "keep me"
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" },
                    new CdcColumnMapping { Column = "qty", Name = "Quantity" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Lines": [
                            {
                                "Product": "SuperWidget",
                                "ExtraInfo": "retain this",
                                "Quantity": 5
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "attr_key", Name = "Key" },
                    new CdcColumnMapping { Column = "attr_value", Name = "Value" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Attributes": {
                            "color": {
                                "Value": "blue",
                                "Source": "user-input"
                            }
                        }
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "carrier", Name = "Carrier" },
                    new CdcColumnMapping { Column = "tracking_no", Name = "TrackingNumber" }
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "ShippingInfo": {
                            "Carrier": "UPS",
                            "TrackingNumber": "ABC123",
                            "EstimatedDate": "2026-04-01"
                        }
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(context, doc.Data, """
                    {
                        "CustomerName": "Bob",
                        "Amount": 75.0
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.Null(doc);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);

                // Old data is gone (delete cleared it), but new embed is applied on a stub
                doc.Data.TryGet("CustomerName", out string name);
                Assert.Null(name);

                AssertDocumentMatches(context, doc.Data, """
                    {
                        "Lines": [
                            {
                                "Product": "NewProduct"
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(context, doc.Data, """
                    {
                        "CustomerName": "Charlie",
                        "Lines": [
                            {
                                "Product": "Widget"
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);
                // Only the embed after the delete survives
                AssertDocumentMatches(context, doc.Data, """
                    {
                        "Lines": [
                            {
                                "Product": "Cherries"
                            }
                        ]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
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

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, "Orders/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(context, doc.Data, """
                    {
                        "OrderId": 1,
                        "CustomerName": "Alice",
                        "Amount": 99.5,
                        "Status": "Confirmed"
                    }
                    """);
            }
        }

        /// <summary>
        /// Helper to create a 2-level deep embedded processor for the pattern:
        /// Company -> Departments[] -> Employees[]
        /// </summary>
        private static (CdcSinkTableConfig RootConfig, CdcSinkEmbeddedTableConfig DeptConfig, CdcSinkEmbeddedTableConfig EmpConfig, CdcSinkTableProcessor EmpProcessor)
            CreateDeepNestedConfig()
        {
            var empConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "employees",
                PropertyName = "Employees",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "emp_id", Name = "EmpId" },
                    new CdcColumnMapping { Column = "emp_name", Name = "EmpName" }
                },
                PrimaryKeyColumns = new List<string> { "emp_id" },
                JoinColumns = new List<string> { "dept_id" },
                Type = CdcSinkRelationType.Array
            };

            var deptConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "departments",
                PropertyName = "Departments",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "dept_id", Name = "DeptId" },
                    new CdcColumnMapping { Column = "dept_name", Name = "DeptName" }
                },
                PrimaryKeyColumns = new List<string> { "dept_id" },
                JoinColumns = new List<string> { "company_id" },
                Type = CdcSinkRelationType.Array,
                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig> { empConfig }
            };

            var rootConfig = new CdcSinkTableConfig
            {
                CollectionName = "Companies",
                SourceTableSchema = "public",
                SourceTableName = "companies",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "company_id", Name = "CompanyId" },
                    new CdcColumnMapping { Column = "company_name", Name = "CompanyName" }
                },
                PrimaryKeyColumns = new List<string> { "company_id" },
                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig> { deptConfig }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { rootConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var empProcessor = docProcessor.GetProcessor("public", "employees");

            return (rootConfig, deptConfig, empConfig, empProcessor);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task DeepNesting_NavigateToEmbeddedParent_PreservesExistingArrayItems()
        {
            // Bug 4: When NavigateToEmbeddedParent traverses an intermediate array and no
            // element matches the FK, it was replacing the entire array with an empty object,
            // losing all existing items.
            //
            // Scenario: Company has Departments[] with two departments.
            // An employee CDC row arrives for dept_id=999 (no matching department).
            // The fix should NOT destroy the existing departments.
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create a company with two departments
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext setupCtx))
            using (var tx = setupCtx.OpenWriteTransaction())
            {
                using var blittable = setupCtx.Sync.ReadForMemory("""
                    {
                        "CompanyId": 1,
                        "CompanyName": "Acme Corp",
                        "Departments": [
                            {
                                "DeptId": 10,
                                "DeptName": "Engineering",
                                "Employees": [
                                    { "EmpId": 100, "EmpName": "Alice" }
                                ]
                            },
                            {
                                "DeptId": 20,
                                "DeptName": "Sales",
                                "Employees": []
                            }
                        ],
                        "@metadata": { "@collection": "Companies" }
                    }
                    """, "test");
                database.DocumentsStorage.Put(setupCtx, "Companies/1", null, blittable);
                tx.Commit();
            }

            var (rootConfig, deptConfig, empConfig, empProcessor) = CreateDeepNestedConfig();

            // Employee row referencing a non-existent department (dept_id=999)
            var empData = new DynamicJsonValue
            {
                ["EmpId"] = 500,
                ["EmpName"] = "NewHire"
            };
            var rawData = new Dictionary<string, object>
            {
                { "company_id", 1 },
                { "dept_id", 999 },
                { "emp_id", 500 },
                { "emp_name", "NewHire" }
            };

            var embedOp = new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.EmbeddedModify,
                DocumentId = "Companies/1",
                Processor = empProcessor,
                MappedData = empData,
                RawValues = ToRawValues(rawData, empProcessor),
                Operation = CdcSinkOperation.Upsert
            };

            var command = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { embedOp },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Companies/1");
                Assert.NotNull(doc);

                // The Departments array must still exist and retain both original departments.
                // Before the fix, the array was replaced with an empty object {}.
                doc.Data.TryGet("Departments", out BlittableJsonReaderArray departments);
                Assert.NotNull(departments);
                Assert.True(departments.Length >= 2,
                    $"Expected at least 2 departments but got {departments.Length}. " +
                    "The existing array items were likely destroyed when no matching element was found.");

                // 3 departments: the 2 originals + a stub created for the unmatched dept_id=99 embed
                Assert.Equal(3, departments.Length);

                // Original departments preserved
                var dept0 = departments[0] as BlittableJsonReaderObject;
                dept0.TryGet("DeptName", out string deptName0);
                Assert.Equal("Engineering", deptName0);

                var dept1 = departments[1] as BlittableJsonReaderObject;
                dept1.TryGet("DeptName", out string deptName1);
                Assert.Equal("Sales", deptName1);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task DeepNesting_ArrayNavigation_MatchingElement_AppliesCorrectly()
        {
            // Bug 3 verification: ApplyArrayOperation receives the correct navigated parent
            // (not the root doc) for 2-level nesting. This test verifies that adding an
            // employee to an existing department correctly navigates to that department
            // and places the employee in its Employees array.
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create company with one department
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext setupCtx))
            using (var tx = setupCtx.OpenWriteTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["CompanyId"] = 1,
                    ["CompanyName"] = "Acme Corp",
                    ["Departments"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue
                        {
                            ["DeptId"] = 10L,
                            ["DeptName"] = "Engineering",
                            ["Employees"] = new DynamicJsonArray
                            {
                                new DynamicJsonValue { ["EmpId"] = 100L, ["EmpName"] = "Alice" }
                            }
                        }
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Companies"
                    }
                };
                using var blittable = setupCtx.ReadObject(djv, "Companies/1");
                database.DocumentsStorage.Put(setupCtx, "Companies/1", null, blittable);
                tx.Commit();
            }

            var (rootConfig, deptConfig, empConfig, empProcessor) = CreateDeepNestedConfig();

            // Add a new employee to the existing Engineering department (dept_id=10)
            var empData = new DynamicJsonValue
            {
                ["EmpId"] = 200,
                ["EmpName"] = "Bob"
            };
            var rawData = new Dictionary<string, object>
            {
                { "company_id", 1 },
                { "dept_id", 10 },
                { "emp_id", 200 },
                { "emp_name", "Bob" }
            };

            var embedOp = new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.EmbeddedModify,
                DocumentId = "Companies/1",
                Processor = empProcessor,
                MappedData = empData,
                RawValues = ToRawValues(rawData, empProcessor),
                Operation = CdcSinkOperation.Upsert
            };

            var command = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { embedOp },
                "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Companies/1");
                Assert.NotNull(doc);

                doc.Data.TryGet("Departments", out BlittableJsonReaderArray departments);
                Assert.NotNull(departments);
                Assert.Equal(1, departments.Length);

                var engDept = (BlittableJsonReaderObject)departments[0];
                engDept.TryGet("DeptName", out string deptName);
                Assert.Equal("Engineering", deptName);

                engDept.TryGet("Employees", out BlittableJsonReaderArray employees);
                Assert.NotNull(employees);
                Assert.Equal(2, employees.Length);

                // Verify both employees are present
                var names = new List<string>();
                for (int i = 0; i < employees.Length; i++)
                {
                    var emp = (BlittableJsonReaderObject)employees[i];
                    emp.TryGet("EmpName", out string empName);
                    names.Add(empName);
                }
                names.Sort();
                Assert.Equal("Alice", names[0]);
                Assert.Equal("Bob", names[1]);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task DeleteThenEmbedOnly_DocumentResurrectedAsStub()
        {
            // Bug 8 analysis: When operations arrive as [Delete, EmbeddedModify] in a single
            // batch, the EmbeddedModify clears the delete state and creates a stub document.
            // This is by design for CDC streams where events are ordered: an embedded modify
            // after a delete means the source DB had the child row recreated after deletion.
            //
            // This test documents the intentional behavior. If the embedded operation arrives
            // AFTER a delete in the same batch, the document is resurrected as a stub with
            // only the embedded data (no root properties preserved).
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Pre-create a document with root properties and an embedded array
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext setupCtx))
            using (var tx = setupCtx.OpenWriteTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["OrderId"] = 1,
                    ["CustomerName"] = "Alice",
                    ["Amount"] = 100.0,
                    ["Lines"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["LineId"] = 1L, ["Product"] = "ExistingProduct" }
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Orders"
                    }
                };
                using var blittable = setupCtx.ReadObject(djv, "Orders/1");
                database.DocumentsStorage.Put(setupCtx, "Orders/1", null, blittable);
                tx.Commit();
            }

            var embeddedConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableName = "order_lines",
                PropertyName = "Lines",
                Type = CdcSinkRelationType.Array,
                JoinColumns = new List<string> { "order_id" },
                PrimaryKeyColumns = new List<string> { "line_id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "line_id", Name = "LineId" },
                    new CdcColumnMapping { Column = "product", Name = "Product" }
                }
            };
            var embProcessor = CreateEmbeddedProcessor(embeddedConfig);

            // Sequence: [Delete, EmbeddedModify] — delete then embed in same batch
            var delete = CreateDeleteOp("Orders/1");
            var embed = CreateEmbeddedOp("Orders/1", new DynamicJsonValue
            {
                ["LineId"] = 99L,
                ["Product"] = "Resurrected"
            }, CdcSinkOperation.Upsert, embProcessor);

            var ops = new List<CdcSinkDocumentOp> { delete, embed };

            var command = new CdcSinkBatchCommand(
                database, ops, "test-config", null,
                tableLoadUpdates: null, patchRequest: null, statsScope: null, statistics: null, logger: null);

            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Companies/1");

                // The document is resurrected as a stub. The delete was overridden.
                // All root properties from the original document are gone.
                doc = database.DocumentsStorage.Get(readCtx, "Orders/1");
                Assert.NotNull(doc);

                // No root data on stub
                doc.Data.TryGet("CustomerName", out string name);
                Assert.Null(name);

                // Only the embedded data from the post-delete operation survives
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "Lines": [
                            {
                                "Product": "Resurrected"
                            }
                        ]
                    }
                    """);
            }
        }

        /// <summary>
        /// INSERT→UPDATE→DELETE→INSERT→UPDATE with audit put() patches.
        /// IgnoreDeletes = false: document is deleted then re-created.
        /// All 5 audit entries must be written (patches have side effects on other docs).
        /// The final document should have Name = "Delta" with no leftover from the pre-delete era.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task InsertUpdateDeleteInsertUpdate_AuditPut_IgnoreDeletesFalse()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = new CdcSinkTableConfig
            {
                CollectionName = "Items",
                SourceTableSchema = "public",
                SourceTableName = "items",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" }
                },
                Patch = @"
                    var op = $old ? 'Update' : 'Insert';
                    put('AuditLog/' + op + '/' + $row.name, {
                        Op: op,
                        Name: $row.name,
                        PreviousName: $old ? $old.Name : null,
                        '@metadata': { '@collection': 'AuditLog' }
                    });",
                OnDelete = new CdcSinkOnDeleteConfig
                {
                    Patch = @"
                        put('AuditLog/Delete/' + $row.name, {
                            Op: 'Delete',
                            Name: $row.name,
                            '@metadata': { '@collection': 'AuditLog' }
                        });"
                }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-audit",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "items");

            DynamicJsonValue MakeMapped(string name) => new DynamicJsonValue
            {
                ["Id"] = 1, ["Name"] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Items" }
            };

            Dictionary<string, object> MakeRaw(string name) => new Dictionary<string, object>
            {
                { "id", 1 }, { "name", name }
            };

            // INSERT(Alpha), UPDATE(Beta), DELETE(Beta), INSERT(Gamma), UPDATE(Delta)
            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Items/1", MakeMapped("Alpha"), MakeRaw("Alpha"), processor),
                CreatePutOp("Items/1", MakeMapped("Beta"), MakeRaw("Beta"), processor),
                CreateDeleteOp("Items/1", processor, MakeRaw("Beta")),
                CreatePutOp("Items/1", MakeMapped("Gamma"), MakeRaw("Gamma"), processor),
                CreatePutOp("Items/1", MakeMapped("Delta"), MakeRaw("Delta"), processor),
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-audit", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                // Final document should be Delta (not deleted, no pre-delete patches applied)
                var doc = database.DocumentsStorage.Get(ctx, "Items/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "Name": "Delta"
                    }
                    """);

                // All 5 audit entries should exist: Insert(Alpha), Update(Beta), Delete(Beta), Insert(Gamma), Update(Delta)
                var auditDocs = database.DocumentsStorage.GetDocumentsStartingWith(ctx, "AuditLog/", null, null, null, 0, 100, token: TestContext.Current.CancellationToken);
                var audits = auditDocs.ToList();
                Assert.Equal(5, audits.Count);

                var auditOps = new List<string>();
                foreach (var audit in audits)
                {
                    audit.Data.TryGet("Op", out string op);
                    auditOps.Add(op);
                }

                Assert.Equal(2, auditOps.Count(x => x == "Insert"));
                Assert.Equal(2, auditOps.Count(x => x == "Update"));
                Assert.Equal(1, auditOps.Count(x => x == "Delete"));
            }
        }

        /// <summary>
        /// INSERT→UPDATE→DELETE→INSERT→UPDATE with audit put() patches.
        /// IgnoreDeletes = true: the Delete is skipped (archive pattern), but audit entries still record it.
        /// Final document should be "Delta" (last update wins regardless of archive).
        /// All 5 audit entries must be written.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task InsertUpdateDeleteInsertUpdate_AuditPut_IgnoreDeletesTrue()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = new CdcSinkTableConfig
            {
                CollectionName = "Items",
                SourceTableSchema = "public",
                SourceTableName = "items",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" }
                },
                Patch = @"
                    var op = $old ? 'Update' : 'Insert';
                    put('AuditLog/' + op + '/' + $row.name, {
                        Op: op,
                        Name: $row.name,
                        PreviousName: $old ? $old.Name : null,
                        '@metadata': { '@collection': 'AuditLog' }
                    });",
                OnDelete = new CdcSinkOnDeleteConfig
                {
                    IgnoreDeletes = true,
                    Patch = @"
                        put('AuditLog/Delete/' + $row.name, {
                            Op: 'Delete',
                            Name: $row.name,
                            '@metadata': { '@collection': 'AuditLog' }
                        });"
                }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-audit-ignore",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "items");

            DynamicJsonValue MakeMapped(string name) => new DynamicJsonValue
            {
                ["Id"] = 1, ["Name"] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Items" }
            };

            Dictionary<string, object> MakeRaw(string name) => new Dictionary<string, object>
            {
                { "id", 1 }, { "name", name }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Items/1", MakeMapped("Alpha"), MakeRaw("Alpha"), processor),
                CreatePutOp("Items/1", MakeMapped("Beta"), MakeRaw("Beta"), processor),
                CreateDeleteOp("Items/1", processor, MakeRaw("Beta")),
                CreatePutOp("Items/1", MakeMapped("Gamma"), MakeRaw("Gamma"), processor),
                CreatePutOp("Items/1", MakeMapped("Delta"), MakeRaw("Delta"), processor),
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-audit-ignore", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Items/1");
                Assert.NotNull(doc);
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "Name": "Delta"
                    }
                    """);

                var auditDocs = database.DocumentsStorage.GetDocumentsStartingWith(ctx, "AuditLog/", null, null, null, 0, 100, token: TestContext.Current.CancellationToken);
                var audits = auditDocs.ToList();

                Assert.Equal(5, audits.Count);

                var auditOps = new List<string>();
                foreach (var audit in audits)
                {
                    audit.Data.TryGet("Op", out string op);
                    auditOps.Add(op);
                }

                // With IgnoreDeletes=true, the document survives — Gamma's $old is the
                // surviving doc (not null), so the audit records it as Update, not Insert.
                // Insert(Alpha), Update(Beta), Delete(Beta), Update(Gamma), Update(Delta)
                Assert.Equal(1, auditOps.Count(x => x == "Insert"));
                Assert.Equal(3, auditOps.Count(x => x == "Update"));
                Assert.Equal(1, auditOps.Count(x => x == "Delete"));
            }
        }

        /// <summary>
        /// INSERT→UPDATE→DELETE→INSERT→UPDATE with this.Count++ patch.
        /// IgnoreDeletes = false: document is deleted then re-created.
        /// Pre-delete patches (Insert Count++, Update Count++) must NOT carry forward.
        /// Final Count should be 2 (Gamma + Delta only), not 4 or 5.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task InsertUpdateDeleteInsertUpdate_Counter_IgnoreDeletesFalse()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = new CdcSinkTableConfig
            {
                CollectionName = "Items",
                SourceTableSchema = "public",
                SourceTableName = "items",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" }
                },
                Patch = "this.Count = (this.Count || 0) + 1;",
                OnDelete = new CdcSinkOnDeleteConfig()
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-counter",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "items");

            DynamicJsonValue MakeMapped(string name) => new DynamicJsonValue
            {
                ["Id"] = 1, ["Name"] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Items" }
            };

            Dictionary<string, object> MakeRaw(string name) => new Dictionary<string, object>
            {
                { "id", 1 }, { "name", name }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Items/1", MakeMapped("Alpha"), MakeRaw("Alpha"), processor),
                CreatePutOp("Items/1", MakeMapped("Beta"), MakeRaw("Beta"), processor),
                CreateDeleteOp("Items/1", processor, MakeRaw("Beta")),
                CreatePutOp("Items/1", MakeMapped("Gamma"), MakeRaw("Gamma"), processor),
                CreatePutOp("Items/1", MakeMapped("Delta"), MakeRaw("Delta"), processor),
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-counter", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Items/1");
                Assert.NotNull(doc);
                // Count should be 2 (Gamma + Delta), NOT 4 — pre-delete patches are flushed
                // on the deleted doc and don't carry forward to the resurrected document.
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "Name": "Delta",
                        "Count": 2
                    }
                    """);
            }
        }

        /// <summary>
        /// INSERT→UPDATE→DELETE→INSERT→UPDATE with this.Count++ patch.
        /// IgnoreDeletes = true: Delete is skipped. The OnDelete patch also does Count++.
        ///
        /// Why Count = 2 and not 5:
        /// The batch command processes ops in order on the same document ID. When a Delete
        /// op arrives with IgnoreDeletes = true, the engine flushes all accumulated patches
        /// (Alpha Count++, Beta Count++, OnDelete Count++) against the pre-delete document
        /// snapshot, then discards the result (the document is not actually deleted, but
        /// With IgnoreDeletes=true, the document survives the delete and the OnDelete
        /// patch runs on the surviving document. All 5 Count++ patches accumulate:
        /// Alpha(1) + Beta(2) + OnDelete(3) + Gamma(4) + Delta(5) = Count=5.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task InsertUpdateDeleteInsertUpdate_Counter_IgnoreDeletesTrue()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = new CdcSinkTableConfig
            {
                CollectionName = "Items",
                SourceTableSchema = "public",
                SourceTableName = "items",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "name", Name = "Name" }
                },
                Patch = "this.Count = (this.Count || 0) + 1;",
                OnDelete = new CdcSinkOnDeleteConfig
                {
                    IgnoreDeletes = true,
                    Patch = "this.Count = (this.Count || 0) + 1;"
                }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-counter-ignore",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "items");

            DynamicJsonValue MakeMapped(string name) => new DynamicJsonValue
            {
                ["Id"] = 1, ["Name"] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Items" }
            };

            Dictionary<string, object> MakeRaw(string name) => new Dictionary<string, object>
            {
                { "id", 1 }, { "name", name }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Items/1", MakeMapped("Alpha"), MakeRaw("Alpha"), processor),
                CreatePutOp("Items/1", MakeMapped("Beta"), MakeRaw("Beta"), processor),
                CreateDeleteOp("Items/1", processor, MakeRaw("Beta")),
                CreatePutOp("Items/1", MakeMapped("Gamma"), MakeRaw("Gamma"), processor),
                CreatePutOp("Items/1", MakeMapped("Delta"), MakeRaw("Delta"), processor),
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-counter-ignore", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(command);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Items/1");
                Assert.NotNull(doc);
                // Count should be 5: all patches accumulate because IgnoreDeletes keeps the
                // document alive. Alpha(1) + Beta(2) + OnDelete(3) + Gamma(4) + Delta(5).
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "Name": "Delta",
                        "Count": 5
                    }
                    """);
            }
        }

        /// <summary>
        /// A patch that exceeds the MaxStepsForScript limit should fail only for
        /// that document, not block the entire CDC batch. Other documents in the
        /// same batch must still be processed and the LSN must still advance.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task PatchMaxStepsExceeded_FailsDocumentNotBatch()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxStepsForScript)] = "50"
            });
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Two tables: one with infinite-loop patch (will hit MaxSteps), one without patch
            var badConfig = CreateRootTableConfig("BadOrders", patch: "while(true) {}");
            badConfig.SourceTableName = "bad_orders";
            var goodConfig = CreateRootTableConfig("GoodOrders");
            goodConfig.SourceTableName = "good_orders";

            // Build the combined patch request from a config that includes the bad table
            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-maxsteps",
                Tables = new List<CdcSinkTableConfig> { badConfig, goodConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var badProcessor = docProcessor.GetProcessor("public", "bad_orders");
            var goodProcessor = docProcessor.GetProcessor("public", "good_orders");

            var badData = new DynamicJsonValue
            {
                ["OrderId"] = 1,
                ["CustomerName"] = "InfiniteLoop",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "BadOrders" }
            };
            var badRaw = new Dictionary<string, object>
            {
                { "order_id", 1 }, { "customer_name", "InfiniteLoop" }, { "amount", 0 }
            };

            var goodData = new DynamicJsonValue
            {
                ["OrderId"] = 2,
                ["CustomerName"] = "Alice",
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "GoodOrders" }
            };
            var goodRaw = new Dictionary<string, object>
            {
                { "order_id", 2 }, { "customer_name", "Alice" }, { "amount", 99 }
            };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("BadOrders/1", badData, badRaw, badProcessor),
                CreatePutOp("GoodOrders/2", goodData, goodRaw, goodProcessor),
            };

            var statistics = new CdcSinkProcessStatistics("test", "test", database.NotificationCenter);

            var command = new CdcSinkBatchCommand(database, ops, "test-maxsteps", "test-lsn",
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: statistics, logger: null);
            await database.TxMerger.Enqueue(command);

            // The good document should succeed despite the other document hitting MaxSteps
            Assert.Equal(1, command.ProcessedSuccessfully);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var good = database.DocumentsStorage.Get(readCtx, "GoodOrders/2");
                Assert.NotNull(good);
                AssertDocumentMatches(readCtx, good.Data, """
                    {
                        "CustomerName": "Alice"
                    }
                    """);

                // The bad document should NOT have been saved
                var bad = database.DocumentsStorage.Get(readCtx, "BadOrders/1");
                Assert.Null(bad);
            }

            // The error should be recorded in statistics
            Assert.Equal(1, statistics.ConsumeErrors);
        }

        /// <summary>
        /// Verifies that NormalizeForJson handles complex types correctly:
        /// - JSON columns (explicitly marked) strings → parsed into native DynamicJsonValue
        /// - Arrays → DynamicJsonArray
        /// - Plain strings stay as strings
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task ComplexTypes_JsonAndArrays_StoredAsNativeJson()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var config = new CdcSinkTableConfig
            {
                CollectionName = "Records",
                SourceTableSchema = "public",
                SourceTableName = "records",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "Id" },
                    new CdcColumnMapping { Column = "metadata", Name = "Metadata", Type = CdcColumnType.Json },
                    new CdcColumnMapping { Column = "tags", Name = "Tags" },
                    new CdcColumnMapping { Column = "scores", Name = "Scores" },
                    new CdcColumnMapping { Column = "plain_text", Name = "PlainText" }
                },
                PrimaryKeyColumns = new List<string> { "id" }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { config }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "records");

            // Simulate Npgsql-returned types:
            // json/jsonb → string containing JSON
            // text[] → string[]
            // integer[] → int[] (but normalized to long[] via NormalizeReaderValue)
            // plain text → stays as string
            var rawData = new Dictionary<string, object>
            {
                { "id", (long)1 },
                { "metadata", """{"key": "value", "count": 42}""" },     // JSON string → should be parsed
                { "tags", new string[] { "alpha", "beta", "gamma" } },   // string array → JSON array
                { "scores", new long[] { 10, 20, 30 } },                  // numeric array → JSON array
                { "plain_text", "just a regular string" },                // plain string → stays as string
            };

            DynamicJsonValue mappedData;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonCtx))
            {
                var (colNames, colValues) = DictToValues(rawData);
                processor.SetSourceColumnNames(colNames);
                mappedData = processor.MapColumns(colValues, jsonCtx);
                mappedData[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Records" };

                var ops = new List<CdcSinkDocumentOp>
                {
                    CreatePutOp("Records/1", mappedData, rawData, processor)
                };

                var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
                await database.TxMerger.Enqueue(command);
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Records/1");
                Assert.NotNull(doc);

                // JSON string should be stored as a nested object, not a string
                Assert.True(doc.Data.TryGetMember("Metadata", out var metadata));
                Assert.IsType<BlittableJsonReaderObject>(metadata);
                var metadataObj = (BlittableJsonReaderObject)metadata;
                metadataObj.TryGet("key", out string key);
                Assert.Equal("value", key);
                metadataObj.TryGet("count", out long count);
                Assert.Equal(42, count);

                // String array should be stored as a JSON array
                Assert.True(doc.Data.TryGetMember("Tags", out var tags));
                Assert.IsType<BlittableJsonReaderArray>(tags);
                var tagsArr = (BlittableJsonReaderArray)tags;
                Assert.Equal(3, tagsArr.Length);
                Assert.Equal("alpha", tagsArr[0].ToString());

                // Numeric array should be stored as a JSON array
                Assert.True(doc.Data.TryGetMember("Scores", out var scores));
                Assert.IsType<BlittableJsonReaderArray>(scores);
                var scoresArr = (BlittableJsonReaderArray)scores;
                Assert.Equal(3, scoresArr.Length);

                // Plain text stays as a string
                doc.Data.TryGet("PlainText", out string plainText);
                Assert.Equal("just a regular string", plainText);
            }
        }

        /// <summary>
        /// Verifies that OnDelete.Patch has access to the document's properties via 'this'.
        /// This is the archive/soft-delete pattern: when a row is deleted in SQL, the patch
        /// copies the document's fields into a separate "deleted" document before the delete proceeds.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks)]
        public async Task OnDeletePatch_CanAccessDocumentProperties()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = new CdcSinkTableConfig
            {
                CollectionName = "Orders",
                SourceTableSchema = "public",
                SourceTableName = "orders",
                PrimaryKeyColumns = new List<string> { "order_id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                    new CdcColumnMapping { Column = "customer_name", Name = "Customer" },
                    new CdcColumnMapping { Column = "amount", Name = "Total" }
                },
                OnDelete = new CdcSinkOnDeleteConfig
                {
                    Patch = @"
                        put('DeletedOrders/' + this.OrderId, {
                            OriginalId: id(this),
                            Customer: this.Customer,
                            Total: this.Total,
                            DeletedAt: new Date().toISOString(),
                            '@metadata': { '@collection': 'DeletedOrders' }
                        });"
                }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-ondelete-this",
                Tables = new List<CdcSinkTableConfig> { tableConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var processor = docProcessor.GetProcessor("public", "orders");

            // Step 1: Create the document via a Put
            var putMapped = new DynamicJsonValue
            {
                ["OrderId"] = (long)42,
                ["Customer"] = "Acme Corp",
                ["Total"] = 1500.0,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Orders" }
            };

            var putRaw = new Dictionary<string, object>
            {
                { "order_id", (long)42 }, { "customer_name", "Acme Corp" }, { "amount", 1500.0 }
            };

            var putOps = new List<CdcSinkDocumentOp>
            {
                CreatePutOp("Orders/42", putMapped, putRaw, processor)
            };

            var putCmd = new CdcSinkBatchCommand(database, putOps, "test-ondelete-this", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Verify the order exists
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext verifyCtx))
            using (verifyCtx.OpenReadTransaction())
            {
                var order = database.DocumentsStorage.Get(verifyCtx, "Orders/42");
                Assert.NotNull(order);
                AssertDocumentMatches(verifyCtx, order.Data, """
                    {
                        "Customer": "Acme Corp"
                    }
                    """);
            }

            // Step 2: Delete the document — the OnDelete.Patch should create a DeletedOrders document
            var deleteRaw = new Dictionary<string, object>
            {
                { "order_id", (long)42 }, { "customer_name", "Acme Corp" }, { "amount", 1500.0 }
            };

            var deleteOps = new List<CdcSinkDocumentOp>
            {
                CreateDeleteOp("Orders/42", processor, deleteRaw)
            };

            var deleteCmd = new CdcSinkBatchCommand(database, deleteOps, "test-ondelete-this", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(deleteCmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                // The original order should be deleted
                var order = database.DocumentsStorage.Get(readCtx, "Orders/42");
                Assert.Null(order);

                // The DeletedOrders document should exist with the original order's properties
                var deleted = database.DocumentsStorage.Get(readCtx, "DeletedOrders/42");
                Assert.NotNull(deleted);
                AssertDocumentMatches(readCtx, deleted.Data, """
                    {
                        "OriginalId": "Orders/42",
                        "Customer": "Acme Corp",
                        "Total": 1500
                    }
                    """);
                deleted.Data.TryGet("DeletedAt", out string deletedAt);
                Assert.NotNull(deletedAt);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task EmbeddedOnDelete_Array_OldHasPreviousValue()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Setup: parent document with a "Total" field we'll decrement on delete
            var parentData = new DynamicJsonValue
            {
                ["Total"] = 0,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Orders" }
            };
            var rootConfig = CreateRootTableConfig("Orders");
            rootConfig.EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
            {
                new CdcSinkEmbeddedTableConfig
                {
                    SourceTableSchema = "public",
                    SourceTableName = "order_lines",
                    PropertyName = "Lines",
                    Columns = new List<CdcColumnMapping>
                    {
                        new() { Column = "line_id", Name = "LineId" },
                        new() { Column = "amount", Name = "Amount" }
                    },
                    PrimaryKeyColumns = new List<string> { "line_id" },
                    JoinColumns = new List<string> { "order_id" },
                    Type = CdcSinkRelationType.Array,
                    // On upsert: add the amount to the total. $old lets us subtract the previous value on update.
                    Patch = "this.Total += $row.amount - ($old ? $old.Amount : 0);",
                    OnDelete = new CdcSinkOnDeleteConfig
                    {
                        // On delete: subtract the old amount. $old must be the item being removed.
                        Patch = "this.Total -= $old ? $old.Amount : 0;"
                    }
                }
            };

            var sinkConfig = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { rootConfig } };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var embProcessor = docProcessor.GetProcessor("public", "order_lines");

            // Step 1: Create parent
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Step 2: Insert line with Amount=100, then delete it
            var insertData = new DynamicJsonValue { ["LineId"] = 1, ["Amount"] = 100 };
            var deleteData = new DynamicJsonValue { ["LineId"] = 1 };
            var rawInsert = new Dictionary<string, object> { { "order_id", 1 }, { "line_id", 1 }, { "amount", 100 } };
            var rawDelete = new Dictionary<string, object> { { "order_id", 1 }, { "line_id", 1 }, { "amount", 100 } };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embProcessor, rawInsert),
                CreateEmbeddedOp("Orders/1", deleteData, CdcSinkOperation.Delete, embProcessor, rawDelete),
            };
            var cmd = new CdcSinkBatchCommand(database, ops, "test", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(cmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Orders/1");
                Assert.NotNull(doc);
                // Insert added 100, delete subtracted 100 via $old → Total should be 0
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "Total": 0,
                        "Lines": []
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task EmbeddedOnDelete_Value_OldHasPreviousValue()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var parentData = new DynamicJsonValue
            {
                ["LastRemovedProduct"] = null,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Orders" }
            };
            var rootConfig = CreateRootTableConfig("Orders");
            rootConfig.EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
            {
                new CdcSinkEmbeddedTableConfig
                {
                    SourceTableSchema = "public",
                    SourceTableName = "order_detail",
                    PropertyName = "Detail",
                    Columns = new List<CdcColumnMapping>
                    {
                        new() { Column = "detail_id", Name = "DetailId" },
                        new() { Column = "product", Name = "Product" }
                    },
                    PrimaryKeyColumns = new List<string> { "detail_id" },
                    JoinColumns = new List<string> { "order_id" },
                    Type = CdcSinkRelationType.Value,
                    OnDelete = new CdcSinkOnDeleteConfig
                    {
                        // Capture the old product name before it's removed
                        Patch = "this.LastRemovedProduct = $old ? $old.Product : null;"
                    }
                }
            };

            var sinkConfig = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { rootConfig } };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var embProcessor = docProcessor.GetProcessor("public", "order_detail");

            // Step 1: Create parent
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Step 2: Insert the embedded value, then delete it
            var insertData = new DynamicJsonValue { ["DetailId"] = 1, ["Product"] = "Widget" };
            var deleteData = new DynamicJsonValue { ["DetailId"] = 1 };
            var rawInsert = new Dictionary<string, object> { { "order_id", 1 }, { "detail_id", 1 }, { "product", "Widget" } };
            var rawDelete = new Dictionary<string, object> { { "order_id", 1 }, { "detail_id", 1 }, { "product", "Widget" } };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embProcessor, rawInsert),
                CreateEmbeddedOp("Orders/1", deleteData, CdcSinkOperation.Delete, embProcessor, rawDelete),
            };
            var cmd = new CdcSinkBatchCommand(database, ops, "test", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(cmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Orders/1");
                Assert.NotNull(doc);
                // $old in the OnDelete patch should have had the previous value
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "LastRemovedProduct": "Widget",
                        "Detail": null
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task EmbeddedOnDelete_Map_OldHasPreviousValue()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var parentData = new DynamicJsonValue
            {
                ["RemovedKeys"] = new DynamicJsonArray(),
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    { [Constants.Documents.Metadata.Collection] = "Orders" }
            };
            var rootConfig = CreateRootTableConfig("Orders");
            rootConfig.EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
            {
                new CdcSinkEmbeddedTableConfig
                {
                    SourceTableSchema = "public",
                    SourceTableName = "order_tags",
                    PropertyName = "Tags",
                    Columns = new List<CdcColumnMapping>
                    {
                        new() { Column = "tag_key", Name = "TagKey" },
                        new() { Column = "tag_value", Name = "TagValue" }
                    },
                    PrimaryKeyColumns = new List<string> { "tag_key" },
                    JoinColumns = new List<string> { "order_id" },
                    Type = CdcSinkRelationType.Map,
                    OnDelete = new CdcSinkOnDeleteConfig
                    {
                        // Record the old value that was removed
                        Patch = "if ($old) this.RemovedKeys.push($old.TagValue);"
                    }
                }
            };

            var sinkConfig = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { rootConfig } };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var embProcessor = docProcessor.GetProcessor("public", "order_tags");

            // Step 1: Create parent
            var putCmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { CreatePutOp("Orders/1", parentData) },
                "test", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(putCmd);

            // Step 2: Insert a tag, then delete it
            var insertData = new DynamicJsonValue { ["TagKey"] = "priority", ["TagValue"] = "high" };
            var deleteData = new DynamicJsonValue { ["TagKey"] = "priority" };
            var rawInsert = new Dictionary<string, object> { { "order_id", 1 }, { "tag_key", "priority" }, { "tag_value", "high" } };
            var rawDelete = new Dictionary<string, object> { { "order_id", 1 }, { "tag_key", "priority" }, { "tag_value", "high" } };

            var ops = new List<CdcSinkDocumentOp>
            {
                CreateEmbeddedOp("Orders/1", insertData, CdcSinkOperation.Upsert, embProcessor, rawInsert),
                CreateEmbeddedOp("Orders/1", deleteData, CdcSinkOperation.Delete, embProcessor, rawDelete),
            };
            var cmd = new CdcSinkBatchCommand(database, ops, "test", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(cmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "Orders/1");
                Assert.NotNull(doc);
                // $old in the OnDelete patch should have had the map entry being removed
                AssertDocumentMatches(ctx, doc.Data, """
                    {
                        "RemovedKeys": ["high"]
                    }
                    """);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task PatchWithNullColumn_NullComparison()
        {
            // Verifies that null CDC column values are passed as JsValue.Null to scripts,
            // so $row.column === null evaluates to true (not false as with C# null).
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var tableConfig = CreateRootTableConfig(patch: @"
                this.MiddleNameIsNull = ($row.middle_name === null);
                this.MiddleNameValue = $row.middle_name;
            ");
            var processor = CreateRootProcessor(tableConfig);

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
                { "middle_name", null }  // null column value
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
                AssertDocumentMatches(readCtx, doc.Data, """
                    {
                        "MiddleNameIsNull": true
                    }
                    """);
                // MiddleNameValue should be null (not stored in blittable)
                Assert.False(doc.Data.TryGetMember("MiddleNameValue", out var val) && val != null,
                    "Expected MiddleNameValue to be null or absent");
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task DeepNesting_OnDelete_IgnoreDeletes_OldHasPreviousValue()
        {
            // FindExistingEmbeddedItem looks up config.PropertyName directly on the root document.
            // For depth >= 2 (Company → Departments[] → Employees[]), "Employees" is nested inside
            // a specific Department, not at the root. So FindExistingEmbeddedItem returns null,
            // and $old is null in the OnDelete.Patch for the employee — losing the previous value.
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Seed the document with a company, one department, one employee
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext setupCtx))
            using (var tx = setupCtx.OpenWriteTransaction())
            {
                using var blittable = setupCtx.Sync.ReadForMemory("""
                    {
                        "CompanyId": 1,
                        "CompanyName": "Acme Corp",
                        "FiredEmployees": [],
                        "Departments": [
                            {
                                "DeptId": 10,
                                "DeptName": "Engineering",
                                "Employees": [
                                    { "EmpId": 100, "EmpName": "Alice" }
                                ]
                            }
                        ],
                        "@metadata": { "@collection": "Companies" }
                    }
                    """, "test");
                database.DocumentsStorage.Put(setupCtx, "Companies/1", null, blittable);
                tx.Commit();
            }

            // Build config with OnDelete.IgnoreDeletes + Patch on the depth-2 employee table
            var empConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "employees",
                PropertyName = "Employees",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "emp_id", Name = "EmpId" },
                    new CdcColumnMapping { Column = "emp_name", Name = "EmpName" }
                },
                PrimaryKeyColumns = new List<string> { "emp_id" },
                JoinColumns = new List<string> { "dept_id" },
                Type = CdcSinkRelationType.Array,
                OnDelete = new CdcSinkOnDeleteConfig
                {
                    IgnoreDeletes = true,
                    // Record the fired employee's name. $old must have the previous value.
                    Patch = "this.FiredEmployees.push($old ? $old.EmpName : 'MISSING');"
                }
            };

            var deptConfig = new CdcSinkEmbeddedTableConfig
            {
                SourceTableSchema = "public",
                SourceTableName = "departments",
                PropertyName = "Departments",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "dept_id", Name = "DeptId" },
                    new CdcColumnMapping { Column = "dept_name", Name = "DeptName" }
                },
                PrimaryKeyColumns = new List<string> { "dept_id" },
                JoinColumns = new List<string> { "company_id" },
                Type = CdcSinkRelationType.Array,
                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig> { empConfig }
            };

            var rootConfig = new CdcSinkTableConfig
            {
                CollectionName = "Companies",
                SourceTableSchema = "public",
                SourceTableName = "companies",
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "company_id", Name = "CompanyId" },
                    new CdcColumnMapping { Column = "company_name", Name = "CompanyName" }
                },
                PrimaryKeyColumns = new List<string> { "company_id" },
                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig> { deptConfig }
            };

            var sinkConfig = new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig> { rootConfig }
            };
            var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
            var empProcessor = docProcessor.GetProcessor("public", "employees");

            // Delete employee Alice (emp_id=100) from dept_id=10
            var deleteData = new DynamicJsonValue { ["EmpId"] = 100 };
            var rawDelete = new Dictionary<string, object>
            {
                { "company_id", 1 }, // root join column (denormalized FK)
                { "dept_id", 10 },   // parent join column
                { "emp_id", 100 },
                { "emp_name", "Alice" }
            };

            var deleteOp = new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.EmbeddedModify,
                DocumentId = "Companies/1",
                Processor = empProcessor,
                MappedData = deleteData,
                RawValues = ToRawValues(rawDelete, empProcessor),
                Operation = CdcSinkOperation.Delete
            };

            var cmd = new CdcSinkBatchCommand(database,
                new List<CdcSinkDocumentOp> { deleteOp },
                "test-config", null,
                tableLoadUpdates: null, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
            await database.TxMerger.Enqueue(cmd);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(readCtx, "Companies/1");
                Assert.NotNull(doc);

                // IgnoreDeletes=true means Alice stays in the Employees array.
                // The OnDelete.Patch should have pushed "Alice" (from $old.EmpName) into FiredEmployees.
                // BUG: FindExistingEmbeddedItem doesn't navigate the nested path, so $old is null,
                // and the patch pushes "MISSING" instead of "Alice".
                doc.Data.TryGet("FiredEmployees", out BlittableJsonReaderArray fired);
                Assert.NotNull(fired);
                Assert.Equal(1, fired.Length);
                Assert.Equal("Alice", fired[0].ToString());
            }
        }

    }
}
