using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;


namespace SlowTests.Server.Documents.Indexing;

public class SchemaValidationIndexingTests : RavenTestBase
{
    public SchemaValidationIndexingTests(ITestOutputHelper output) : base(output)
    {
    }

    private const string ValidateNonDocumentDataForMapIndex =
"""
from doc in docs 
select new 
{
    Id = doc.Id,
    Error = SchemaValid(doc.Inner)
}
""";

    private const string ValidateNonDocumentDataForJavascriptMapIndex =
"""
map("TestObjs", (doc) => { 
    return {
        Id: id(doc),
        Error: schemaValidate(doc.Inner)
    };
})
""";

    public static readonly TheoryData<string, IndexType> ValidateNonDocumentData = new TheoryData<string, IndexType>()
    {
        { ValidateNonDocumentDataForMapIndex, IndexType.Map },
        { ValidateNonDocumentDataForJavascriptMapIndex, IndexType.JavaScriptMap }
    };

    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateNonDocumentData))]
    public async Task IndexingSchemaValidity_WhenSchemaValidateNonDocument_ShouldFailIndexing(string map, IndexType indexType)
    {
        using var store = GetDocumentStore();

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        var indexDefinition = new IndexDefinition()
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaValidation = schemaDefinition,
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        await using (var bull = store.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                await bull.StoreAsync(new TestObj { Prop = "0123456789a", Inner = new { Prop = "0123456789a" } });
            }
        }
        
        var errors = Indexes.WaitForIndexingErrors(store);
        
        Assert.NotEmpty(errors);
    }

    private const string ValidateDocumentDataForMapIndex =
        """
        from doc in docs 
        select new 
        {
            Id = doc.Id,
            Error = SchemaValid(doc)
        }
        """;

    private const string ValidateDocumentDataForJavascriptMapIndex =
        """
        map("TestObjs", (doc) => { 
            return {
                Id: id(doc),
                Error: schemaValidate(doc)
            };
        })
        """;

    public static readonly TheoryData<string, IndexType> ValidateDocumentData = new TheoryData<string, IndexType>()
    {
        { ValidateDocumentDataForMapIndex, IndexType.Map },
        { ValidateDocumentDataForJavascriptMapIndex, IndexType.JavaScriptMap }
    };
    
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaValidity_WhenIndexingSchemaError_ShouldGetSchemaValidationErrors(string map, IndexType indexType)
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore();

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaValidation = schemaDefinition,
            Fields = new Dictionary<string, IndexFieldOptions>{{"Error", new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" }, invalidDocId);
            await session.StoreAsync(new TestObj { Prop = "01" }, validDocId);
            await session.SaveChangesAsync();
        }
        
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestObj>(indexDefinition.Name).ProjectInto<IndexResult>().ToArrayAsync();
            var dicResults = results.ToDictionary(o => o.Id);
            Assert.Equal(null, dicResults[validDocId].Error);
            Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", dicResults[invalidDocId].Error);
        }
    }
    

    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaValidity_WhenDefineSchemaOnMetadata_ShouldReject(string map, IndexType indexType)
    {
        using var store = GetDocumentStore();

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaValidation = schemaDefinition,
            Fields = new Dictionary<string, IndexFieldOptions>{{"Error", new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition)));
        Assert.Contains("Define a schema validation on metadata is not allowed", e.Message);
    }

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
        public object Inner { get; set; }
    }
    
    private class IndexResult
    {
        public string Id { get; set; }
        public string Error { get; set; }
    }
}
