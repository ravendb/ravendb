using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task IndexingSchemaValidity_WhenFilteringByNotSchemaValid_ShouldContainsOnlyInvalidDocuments()
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore();

        const string map =
            """
            from doc in docs 
            where SchemaValid(doc) == false
            select new 
            {
                Id = doc.Id
            }
            """;

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
            Name = "MyCounterIndex",
            Maps = { map },
            SchemaValidation = schemaDefinition
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
            var results = await session.Query<TestObj>(indexDefinition.Name).ToArrayAsync();
            var ids = results.Select(o => o.Id).ToArray();
            Assert.Contains(invalidDocId, ids);
            Assert.DoesNotContain(validDocId, ids);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task IndexingSchemaValidity_WhenSchemaValidNonDocument_ShouldFailIndexing()
    {
        using var store = GetDocumentStore();

        const string map =
            """
            from doc in docs 
            where SchemaValid(doc.Inner) == false
            select new 
            {
                Id = doc.Id
            }
            """;

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
            Name = "MyCounterIndex",
            Maps = { map },
            SchemaValidation = schemaDefinition
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        await using (var bull = store.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                await bull.StoreAsync(new TestObj { Prop = "0123456789a" });
            }
        }
        
        var errors = Indexes.WaitForIndexingErrors(store);
        
        Assert.NotEmpty(errors);
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task IndexingSchemaValidity_WhenIndexingSchemaError_ShouldGetSchemaValidationErrors()
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore();

        const string map =
            """
            from doc in docs 
            select new 
            {
                Id = doc.Id,
                Error = SchemaError(doc)
            }
            """;

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
            Name = "MyCounterIndex",
            Maps = { map },
            SchemaValidation = schemaDefinition,
            Fields = new Dictionary<string, IndexFieldOptions>{{"Error", new IndexFieldOptions{Storage = FieldStorage.Yes}}}
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
