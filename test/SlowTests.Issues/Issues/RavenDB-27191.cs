using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27191(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static CdcSinkConfiguration CreateRenamedJunctionConfig(string customerJoinColumn) => new()
    {
        Name = "TestCdc",
        ConnectionStringName = "TestSql",
        Tables =
        [
            new CdcSinkTableConfig
            {
                CollectionName = "CustomersAddressesMapping",
                SourceTableSchema = "public",
                SourceTableName = "CustomerAddresses",
                PrimaryKeyColumns = ["Customer_Id", "Address_Id"],
                Columns =
                [
                    new CdcColumnMapping { Column = "Customer_Id", Name = "CustomerId" },
                    new CdcColumnMapping { Column = "Address_Id", Name = "AddressId" },
                ],
                LinkedTables =
                [
                    new CdcSinkLinkedTableConfig
                    {
                        SourceTableSchema = "public",
                        SourceTableName = "Customer",
                        PropertyName = "Customer",
                        JoinColumns = [customerJoinColumn],
                        LinkedCollectionName = "Customers",
                    },
                    new CdcSinkLinkedTableConfig
                    {
                        SourceTableSchema = "public",
                        SourceTableName = "Address",
                        PropertyName = "Address",
                        JoinColumns = ["Address_Id"],
                        LinkedCollectionName = "Addresses",
                    },
                ],
            },
        ],
    };

    [RavenFact(RavenTestCategory.Sinks)]
    public void UnresolvableJoinColumn_FailsTheSameWayOnEveryAttempt()
    {
        var documentProcessor = new CdcSinkDocumentProcessor(CreateRenamedJunctionConfig(customerJoinColumn: "CustomerId"));
        var names = new[] { "Address_Id", "Customer_Id" };

        var first = Assert.Throws<InvalidOperationException>(
            () => documentProcessor.SetSourceColumnNames("public", "CustomerAddresses", names));
        Assert.Contains("Column 'CustomerId' not found in source columns", first.Message);

        var processor = documentProcessor.GetProcessor("public", "CustomerAddresses");
        Assert.Null(processor.SourceColumnNames);
        Assert.Null(processor.LinkedTableJoinIndices);

        var second = Assert.Throws<InvalidOperationException>(
            () => documentProcessor.SetSourceColumnNames("public", "CustomerAddresses", names));
        Assert.Equal(first.Message, second.Message);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void JoinColumnsNamingSourceColumns_ResolveBothReferences()
    {
        var documentProcessor = new CdcSinkDocumentProcessor(CreateRenamedJunctionConfig(customerJoinColumn: "Customer_Id"));
        documentProcessor.SetSourceColumnNames("public", "CustomerAddresses", ["Address_Id", "Customer_Id"]);

        var row = new CdcSinkRow
        {
            TableSchema = "public",
            TableName = "CustomerAddresses",
            Operation = CdcSinkOperation.Upsert,
            Data = [42, 7]
        };

        var result = documentProcessor.ProcessRow(row, null);

        Assert.NotNull(result);
        Assert.Equal("CustomersAddressesMapping/7/42", result.DocumentId);
        Assert.Equal("Customers/7", result.MappedData["Customer"].ToString());
        Assert.Equal("Addresses/42", result.MappedData["Address"].ToString());
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validate_RejectsLinkedJoinColumnNamingMappedProperty()
    {
        var configuration = CreateRenamedJunctionConfig(customerJoinColumn: "CustomerId");

        Assert.False(configuration.Validate(out var errors, validateName: false, validateConnection: false));
        var error = Assert.Single(errors);
        Assert.Contains("Linked table 'Customer' under 'CustomersAddressesMapping'", error);
        Assert.Contains("join column 'CustomerId' is a mapped property name", error);
        Assert.Contains("use 'Customer_Id' instead", error);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validate_AcceptsJoinColumnNamingSourceColumn()
    {
        var configuration = CreateRenamedJunctionConfig(customerJoinColumn: "Customer_Id");

        Assert.True(configuration.Validate(out var errors, validateName: false, validateConnection: false), string.Join("; ", errors));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validate_AcceptsJoinColumnThatIsNotMapped()
    {
        var configuration = CreateRenamedJunctionConfig(customerJoinColumn: "Customer_Id");
        configuration.Tables[0].LinkedTables[1].JoinColumns = ["Unmapped_Address_Id"];

        Assert.True(configuration.Validate(out var errors, validateName: false, validateConnection: false), string.Join("; ", errors));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validate_RejectsEmbeddedJoinColumnNamingMappedProperty()
    {
        var configuration = new CdcSinkConfiguration
        {
            Name = "TestCdc",
            ConnectionStringName = "TestSql",
            Tables =
            [
                new CdcSinkTableConfig
                {
                    CollectionName = "Orders",
                    SourceTableSchema = "public",
                    SourceTableName = "Order",
                    PrimaryKeyColumns = ["Id"],
                    Columns = [new CdcColumnMapping { Column = "Id", Name = "Id" }],
                    EmbeddedTables =
                    [
                        new CdcSinkEmbeddedTableConfig
                        {
                            SourceTableSchema = "public",
                            SourceTableName = "OrderItem",
                            PropertyName = "Items",
                            Type = CdcSinkRelationType.Array,
                            PrimaryKeyColumns = ["Id"],
                            JoinColumns = ["OrderId"],
                            Columns =
                            [
                                new CdcColumnMapping { Column = "Id", Name = "Id" },
                                new CdcColumnMapping { Column = "Order_Id", Name = "OrderId" },
                            ],
                        },
                    ],
                },
            ],
        };

        Assert.False(configuration.Validate(out var errors, validateName: false, validateConnection: false));
        var error = Assert.Single(errors, e => e.Contains("join column 'OrderId'"));
        Assert.Contains("Embedded table 'OrderItem' under 'Orders'", error);
        Assert.Contains("use 'Order_Id' instead", error);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validate_RejectsEmptyJoinColumn()
    {
        var configuration = CreateRenamedJunctionConfig(customerJoinColumn: "  ");

        Assert.False(configuration.Validate(out var errors, validateName: false, validateConnection: false));
        Assert.Contains("Linked table 'Customer' under 'CustomersAddressesMapping': join column names cannot be empty", errors);
    }
}
