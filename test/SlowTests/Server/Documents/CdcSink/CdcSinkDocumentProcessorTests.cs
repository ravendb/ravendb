using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink;

public class CdcSinkDocumentProcessorTests
{
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
                    Name = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    ColumnsMapping = new Dictionary<string, string>
                    {
                        { "order_id", "OrderId" },
                        { "customer_id", "CustomerId" },
                        { "order_date", "OrderDate" },
                    },
                    LinkedTables = new List<CdcSinkLinkedTableConfig>
                    {
                        new()
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "customers",
                            PropertyName = "Customer",
                            JoinColumns = new List<string> { "customer_id" },
                            Type = CdcSinkRelationType.Value,
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
                            ColumnsMapping = new Dictionary<string, string>
                            {
                                { "product_id", "ProductId" },
                                { "unit_price", "UnitPrice" },
                                { "quantity", "Quantity" },
                                { "discount", "Discount" },
                            },
                        }
                    }
                },
                new()
                {
                    Name = "Customers",
                    SourceTableSchema = "public",
                    SourceTableName = "customers",
                    PrimaryKeyColumns = new List<string> { "customer_id" },
                    ColumnsMapping = new Dictionary<string, string>
                    {
                        { "customer_id", "CustomerId" },
                        { "company_name", "CompanyName" },
                        { "contact_name", "ContactName" },
                    },
                }
            }
        };
    }

    [Fact]
    public void RootUpsert_ProducesCorrectPut()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "customer_id", "ALFKI" },
                { "order_date", "2024-01-15" },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Put, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Upsert, result.Operation);
        Assert.NotNull(result.MappedData);
    }

    [Fact]
    public void RootDelete_ProducesCorrectDelete()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Delete,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Delete, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
    }

    [Fact]
    public void RootWithLink_ProducesLinkedDocumentId()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "customer_id", "ALFKI" },
                { "order_date", "2024-01-15" },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.Put, result.Type);
        // The linked Customer should generate a document ID reference
        Assert.NotNull(result.MappedData["Customer"]);
        Assert.Equal("Customers/ALFKI", result.MappedData["Customer"].ToString());
    }

    [Fact]
    public void EmbeddedUpsert_Array_ProducesEmbeddedModify()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "product_id", 11 },
                { "unit_price", 14.0 },
                { "quantity", 12 },
                { "discount", 0.0 },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Upsert, result.Operation);
        Assert.False(result.Processor.IsRoot);
        Assert.Equal("Lines", result.Processor.EmbeddedConfig.PropertyName);
        Assert.Equal(CdcSinkRelationType.Array, result.Processor.EmbeddedConfig.Type);
    }

    [Fact]
    public void EmbeddedDelete_ProducesEmbeddedModifyWithDelete()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Delete,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "product_id", 11 },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkOperation.Delete, result.Operation);
    }

    [Fact]
    public void UnknownTable_ReturnsNull()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "nonexistent_table",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object> { { "id", 1 } }
        };

        // Unknown tables are gracefully skipped (returns null) instead of throwing,
        // because the publication may cover more tables than the CDC Sink configuration.
        var result = processor.ProcessRow(row);
        Assert.Null(result);
    }

    [Fact]
    public void ColumnMapping_RenamesSqlColumnsToDocumentProperties()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "customers",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "customer_id", "ALFKI" },
                { "company_name", "Alfreds Futterkiste" },
                { "contact_name", "Maria Anders" },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal("Customers/ALFKI", result.DocumentId);
        // Mapped names, not SQL names
        Assert.NotNull(result.MappedData["CompanyName"]);
        Assert.Equal("Alfreds Futterkiste", result.MappedData["CompanyName"].ToString());
        Assert.Equal("Maria Anders", result.MappedData["ContactName"].ToString());
    }

    [Fact]
    public void RawData_ContainsAllColumnsIncludingUnmapped()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "customers",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "customer_id", "ALFKI" },
                { "company_name", "Alfreds Futterkiste" },
                { "contact_name", "Maria Anders" },
                { "phone", "+49 123 456" },       // Not in ColumnsMapping
                { "country", "Germany" },           // Not in ColumnsMapping
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        // RawData should have ALL columns, including unmapped ones
        Assert.NotNull(result.RawData);
        Assert.NotNull(result.RawData["phone"]);
        Assert.Equal("+49 123 456", result.RawData["phone"].ToString());
        Assert.NotNull(result.RawData["country"]);
        Assert.Equal("Germany", result.RawData["country"].ToString());

        // MappedData should NOT have unmapped columns
        Assert.Null(result.MappedData["phone"]);
        Assert.Null(result.MappedData["country"]);
    }

    [Fact]
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
                    Name = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    ColumnsMapping = new Dictionary<string, string>
                    {
                        { "order_id", "OrderId" },
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
                            ColumnsMapping = new Dictionary<string, string>
                            {
                                { "product_id", "ProductId" },
                                { "quantity", "Quantity" },
                            },
                        }
                    }
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "product_id", 11 },
                { "quantity", 12 },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal(CdcSinkRelationType.Map, result.Processor.EmbeddedConfig.Type);
    }

    [Fact]
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
                    Name = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "orders",
                    PrimaryKeyColumns = new List<string> { "order_id" },
                    ColumnsMapping = new Dictionary<string, string>
                    {
                        { "order_id", "OrderId" },
                        { "shipping_id", "ShippingId" },
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
                            ColumnsMapping = new Dictionary<string, string>
                            {
                                { "shipping_id", "ShippingId" },
                                { "carrier", "Carrier" },
                                { "tracking_number", "TrackingNumber" },
                            },
                        }
                    }
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "shipping_info",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "shipping_id", 5 },
                { "carrier", "FedEx" },
                { "tracking_number", "1Z999AA10123456784" },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, result.Type);
        Assert.Equal("Orders/10248", result.DocumentId);
        Assert.Equal(CdcSinkRelationType.Value, result.Processor.EmbeddedConfig.Type);
    }

    [Fact]
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
                    Name = "OrderDetails",
                    SourceTableSchema = "public",
                    SourceTableName = "order_details",
                    PrimaryKeyColumns = new List<string> { "order_id", "product_id" },
                    ColumnsMapping = new Dictionary<string, string>
                    {
                        { "order_id", "OrderId" },
                        { "product_id", "ProductId" },
                        { "quantity", "Quantity" },
                    },
                }
            }
        };

        var processor = new CdcSinkDocumentProcessor(config);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "order_details",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "product_id", 11 },
                { "quantity", 12 },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Equal("OrderDetails/10248/11", result.DocumentId);
    }

    [Fact]
    public void NullLink_ProducesNullProperty()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "orders",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "customer_id", null },
                { "order_date", "2024-01-15" },
            }
        };

        var result = processor.ProcessRow(row);

        Assert.NotNull(result);
        Assert.Null(result.MappedData["Customer"]);
    }

    [Fact]
    public void CaseInsensitiveTableLookup()
    {
        var processor = new CdcSinkDocumentProcessor(CreateOrdersConfig());

        var row = new CdcSinkRow
        {
            TableSchema = "PUBLIC",
            TableName = "ORDERS",
            Operation = CdcSinkOperation.Upsert,
            Data = new Dictionary<string, object>
            {
                { "order_id", 10248 },
                { "customer_id", "ALFKI" },
                { "order_date", "2024-01-15" },
            }
        };

        var result = processor.ProcessRow(row);
        Assert.NotNull(result);
        Assert.Equal("Orders/10248", result.DocumentId);
    }
}
