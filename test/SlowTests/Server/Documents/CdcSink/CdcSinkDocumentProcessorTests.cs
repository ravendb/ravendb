using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink;

public class CdcSinkDocumentProcessorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static (string[] Names, object[] Values) DictToValues(Dictionary<string, object> dict) => CdcSinkBatchCommandTests.DictToValues(dict);

    private static CdcSinkConfiguration CreateOrdersConfig()
    {
        return new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables = new List<CdcSinkTableConfig>
            {
                new()
                {
                    CollectionName = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                        new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" },
                        new CdcColumnMapping { Column = "order_date", Name = "OrderDate" }
                    },
                    LinkedTables = new List<CdcSinkLinkedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "customers",
                            PropertyName = "Customer",
                            JoinColumns = new List<string> { "customer_id" },
                            LinkedCollectionName = "Customers",
                        }
                    },
                    EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "order_details",
                            PropertyName = "Lines",
                            PrimaryKeyColumns = new List<string> { "product_id" },
                            JoinColumns = new List<string> { "order_id" },
                            Type = CdcSinkRelationType.Array,
                            Columns = new List<CdcColumnMapping>
                            {
                                new CdcColumnMapping { Column = "product_id", Name = "ProductId" },
                                new CdcColumnMapping { Column = "unit_price", Name = "UnitPrice" },
                                new CdcColumnMapping { Column = "quantity", Name = "Quantity" },
                                new CdcColumnMapping { Column = "discount", Name = "Discount" }
                            },
                        }
                    }
                },
                new()
                {
                    CollectionName = "Customers",
                    SourceTableSchema = "public",
                    SourceTableName = "customers",
                    PrimaryKeyColumns = new List<string> { "customer_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" },
                        new CdcColumnMapping { Column = "company_name", Name = "CompanyName" },
                        new CdcColumnMapping { Column = "contact_name", Name = "ContactName" }
                    },
                }
            }
        };
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RootUpsert_ProducesCorrectPut()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "customer_id", "ALFKI" },
            { "order_date", "2024-01-15" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "orders", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Put, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Upsert, result.Operation);
        Assert.NotNull(result.MappedData);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RootDelete_ProducesCorrectDelete()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        // Full column set (as providers always supply), with null values for non-PK columns.
        // Deletes only carry PK values, but SourceColumnNames is the full schema.
        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "customer_id", null },
            { "order_date", null },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "orders", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Delete,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Delete, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RootWithLink_ProducesLinkedDocumentId()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "customer_id", "ALFKI" },
            { "order_date", "2024-01-15" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "orders", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Put, result.Type);
        // The linked Customer should generate a document ID reference
        Assert.NotNull(result.MappedData["Customer"]);
        Assert.Equal("Customers/ALFKI", result.MappedData["Customer"].ToString());
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void EmbeddedUpsert_Array_ProducesEmbeddedModify()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "product_id", 11 },
            { "unit_price", 14.0 },
            { "quantity", 12 },
            { "discount", 0.0 },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "order_details", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Upsert, result.Operation);
        Assert.False(result.Processor.IsRoot);
        Assert.Equal("Lines", result.Processor.EmbeddedConfig.PropertyName);
        Assert.Equal(CdcSinkRelationType.Array, result.Processor.EmbeddedConfig.Type);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void EmbeddedDelete_ProducesEmbeddedModifyWithDelete()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        // Full column set — deletes only carry PK values but SourceColumnNames is the full schema
        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "product_id", 11 },
            { "unit_price", null },
            { "quantity", null },
            { "discount", null },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "order_details", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Delete,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Delete, result.Operation);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void UnknownTable_ReturnsNull()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        // Unknown table — no SetSourceColumnNames call (providers only resolve configured tables)
        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "nonexistent_table",
            Operation = CdcSinkOperation.Upsert,
            Data = new object[] { 1 }
        };

        // Unknown tables are gracefully skipped (returns null) instead of throwing,
        // because the publication may cover more tables than the CDC Sink configuration.
        var result = processor.ProcessRow(row, null);
        Assert.Null(result);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void ColumnMapping_RenamesSqlColumnsToDocumentProperties()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "customer_id", "ALFKI" },
            { "company_name", "Alfreds Futterkiste" },
            { "contact_name", "Maria Anders" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "customers", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "customers",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal("Customers/ALFKI", result.DocumentId);
        // Mapped names, not SQL names
        Assert.NotNull(result.MappedData["CompanyName"]);
        Assert.Equal("Alfreds Futterkiste", result.MappedData["CompanyName"].ToString());
        Assert.Equal("Maria Anders", result.MappedData["ContactName"].ToString());
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RawData_ContainsAllColumnsIncludingUnmapped()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "customer_id", "ALFKI" },
            { "company_name", "Alfreds Futterkiste" },
            { "contact_name", "Maria Anders" },
            { "phone", "+49 123 456" },       // Not in ColumnsMapping
            { "country", "Germany" },           // Not in ColumnsMapping
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "customers", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "customers",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        // Verify RawValues contains all columns (including unmapped)
        Assert.NotNull(result.RawValues);
        var sourceNames = result.Processor.SourceColumnNames;
        var rawDict = new Dictionary<string, object>();
        for (int i = 0; i < sourceNames.Length; i++)
            rawDict[sourceNames[i]] = result.RawValues[i];
        Assert.Equal("+49 123 456", rawDict["phone"].ToString());
        Assert.Equal("Germany", rawDict["country"].ToString());

        // MappedData should NOT have unmapped columns
        Assert.Null(result.MappedData["phone"]);
        Assert.Null(result.MappedData["country"]);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void EmbeddedUpsert_Map_ProducesCorrectConfig()
    {
        var config = new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables = new List<CdcSinkTableConfig>
            {
                new()
                {
                    CollectionName = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "order_id", Name = "OrderId" }
                    },
                    EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "order_details",
                            PropertyName = "Lines",
                            PrimaryKeyColumns = new List<string> { "product_id" },
                            JoinColumns = new List<string> { "order_id" },
                            Type = CdcSinkRelationType.Map,
                            Columns = new List<CdcColumnMapping>
                            {
                                new CdcColumnMapping { Column = "product_id", Name = "ProductId" },
                                new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                            },
                        }
                    }
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "product_id", 11 },
            { "quantity", 12 },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "order_details", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal(CdcSinkRelationType.Map, result.Processor.EmbeddedConfig.Type);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void EmbeddedValue_ManyToOne_ProducesCorrectConfig()
    {
        var config = new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables = new List<CdcSinkTableConfig>
            {
                new()
                {
                    CollectionName = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                        new CdcColumnMapping { Column = "shipping_id", Name = "ShippingId" }
                    },
                    EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "shipping_info",
                            PropertyName = "Shipping",
                            PrimaryKeyColumns = new List<string> { "shipping_id" },
                            JoinColumns = new List<string> { "order_id" },
                            Type = CdcSinkRelationType.Value,
                            Columns = new List<CdcColumnMapping>
                            {
                                new CdcColumnMapping { Column = "shipping_id", Name = "ShippingId" },
                                new CdcColumnMapping { Column = "carrier", Name = "Carrier" },
                                new CdcColumnMapping { Column = "tracking_number", Name = "TrackingNumber" }
                            },
                        }
                    }
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "shipping_id", 5 },
            { "carrier", "FedEx" },
            { "tracking_number", "1Z999AA10123456784" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "shipping_info", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "shipping_info",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkRelationType.Value, result.Processor.EmbeddedConfig.Type);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void CompositeKey_GeneratesCorrectDocumentId()
    {
        var config = new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables = new List<CdcSinkTableConfig>
            {
                new()
                {
                    CollectionName = "OrderDetails",
                    SourceTableSchema = "public",
                    SourceTableName = "order_details",
                    PrimaryKeyColumns = new List<string> { "order_id", "product_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                        new CdcColumnMapping { Column = "product_id", Name = "ProductId" },
                        new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                    },
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "product_id", 11 },
            { "quantity", 12 },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "order_details", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal("OrderDetails/10248/11", result.DocumentId);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void NullLink_ProducesNullProperty()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "customer_id", null },
            { "order_date", "2024-01-15" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "orders", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Null(result.MappedData["Customer"]);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void CaseInsensitiveTableLookup()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "customer_id", "ALFKI" },
            { "order_date", "2024-01-15" },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("PUBLIC", "ORDERS", names);

        var row = new CdcSinkRow
        {
            TableSchema = "PUBLIC",
            TableName = "ORDERS",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);
        Assert.NotNull(result);
        Assert.Equal("Orders/10248", result.DocumentId);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void EmbeddedWithLinkedTable_ProducesLinkedDocumentId()
    {
        var config = new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables = new List<CdcSinkTableConfig>
            {
                new()
                {
                    CollectionName = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "order_id", Name = "OrderId" }
                    },
                    EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "order_details",
                            PropertyName = "Lines",
                            PrimaryKeyColumns = new List<string> { "detail_id" },
                            JoinColumns = new List<string> { "order_id" },
                            Type = CdcSinkRelationType.Array,
                            Columns = new List<CdcColumnMapping>
                            {
                                new CdcColumnMapping { Column = "detail_id", Name = "DetailId" },
                                new CdcColumnMapping { Column = "product_id", Name = "ProductId" },
                                new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                            },
                            LinkedTables = new List<CdcSinkLinkedTableConfig>
                            {
                                new()
                                {
                                    SourceTableSchema = "public",
                                    SourceTableName = "products",
                                    PropertyName = "Product",
                                    JoinColumns = new List<string> { "product_id" },
                                    LinkedCollectionName = "Products",
                                }
                            }
                        }
                    }
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var dict = new Dictionary<string, object>
        {
            { "order_id", 10248 },
            { "detail_id", 1 },
            { "product_id", 42 },
            { "quantity", 5 },
        };
        var (names, values) = DictToValues(dict);
        processor.SetSourceColumnNames("public", "order_details", names);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = values
        };

        var result = processor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.NotNull(result.MappedData["Product"]);
        Assert.Equal("Products/42", result.MappedData["Product"].ToString());
    }
}
