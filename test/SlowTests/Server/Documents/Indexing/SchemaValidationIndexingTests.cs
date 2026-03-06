using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
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
    Errors = Schema.GetErrorsFor(doc.Inner)
}
""";

    private const string ValidateNonDocumentDataForJavascriptMapIndex =
"""
map("TestObjs", (doc) => { 
    return {
        Id: id(doc),
        Errors: schema.getErrorsFor(doc.Inner)
    };
})
""";

    public static readonly TheoryData<string, IndexType, string> ValidateNonDocumentData = new TheoryData<string, IndexType, string>()
    {
        { ValidateNonDocumentDataForMapIndex, IndexType.Map, "Schema.GetErrorsFor may only be called with a document" },
        { ValidateNonDocumentDataForJavascriptMapIndex, IndexType.JavaScriptMap, "'schema.GetErrorsFor' can only be performed on the source document." }
    };

    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateNonDocumentData))]
    public async Task IndexingSchemaErrors_WhenSchemaValidateNonDocument_ShouldFailIndexing(string map, IndexType indexType, string errorMsg)
    {
        using var store = GetDocumentStore();
        
        var collection = store.Conventions.FindCollectionName(typeof(TestObj));
        
        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }
        var schemaDefinitions = new IndexSchemaDefinitions { { collection, schemaDefinition } };
        var indexDefinition = new IndexDefinition()
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaDefinitions = schemaDefinitions,
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
        
        var indexErrors = Indexes.WaitForIndexingErrors(store);
        Assert.NotEmpty(indexErrors);
        
        var errors = indexErrors.First().Errors;
        Assert.NotEmpty(errors);
        Assert.Contains(errors, error => error.Error.Contains(errorMsg));
    }

    private const string ValidateDocumentDataForMapIndex =
        """
        from doc in docs
        where MetadataFor(doc)["@collection"] != "@hilo"
        select new 
        {
            Id = doc.Id,
            Errors = Schema.GetErrorsFor(doc)
        }
        """;

    private const string ValidateDocumentDataForJavascriptMapIndex =
        """
        map("@all_docs", (doc) => { 
            let metadata = doc["@metadata"];
            if( metadata == null || metadata["@collection"] !== "@hilo")
                return {
                    Id: id(doc),
                    Errors: schema.getErrorsFor(doc)
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
    public async Task IndexingSchemaErrors_WhenFailsOneRule_ShouldGetTheError(string map, IndexType indexType)
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore();
        var collection = store.Conventions.FindCollectionName(typeof(TestObj));
        
        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        var schemaDefinitions = new IndexSchemaDefinitions { { collection, schemaDefinition } };
        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaDefinitions = schemaDefinitions,
            Fields = new Dictionary<string, IndexFieldOptions>{{nameof(IndexResult.Errors), new IndexFieldOptions{Storage = FieldStorage.Yes}}},
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
            Assert.Null(dicResults[validDocId].Errors);

            var errors = dicResults[invalidDocId].Errors;
            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", errors.First());
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaErrors_WhenFailsMultipleRules_ShouldGetTheErrors(string map, IndexType indexType)
    {
        var expectedErrors = new[]
        {
            "The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.",
            "The pattern of the value '0123456789a' at 'Prop' does not match the required pattern '^something'.",
            "The required property 'Prop1' is missing at ''."
        };
        
        using var store = GetDocumentStore();
        var collection = store.Conventions.FindCollectionName(typeof(TestObj));
        
        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { 
                        [SVC.Properties] = new DynamicJsonValue
                        {
                            ["Prop"] = new DynamicJsonValue { [SVC.MaxLength] = 10, [SVC.Pattern] = "^something",   },
                        },
                        [SVC.Required] = new [] {"Prop1"}
                    },
                    "schema-validation-configuration").ToString();
        }

        var schemaDefinitions = new IndexSchemaDefinitions { { collection, schemaDefinition } };
        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaDefinitions = schemaDefinitions,
            Fields = new Dictionary<string, IndexFieldOptions>{{nameof(IndexResult.Errors), new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" });
            await session.StoreAsync(new TestObj { Prop = "0123456789a" });
            await session.SaveChangesAsync();
        }
        
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestObj>(indexDefinition.Name).ProjectInto<IndexResult>().ToArrayAsync();
            Assert.All(results, result =>
            {
                Assert.NotNull(result.Errors);
                Assert.Equivalent(expectedErrors, result.Errors);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaErrors_WhenDefineSchemaOnMetadata_ShouldReject(string map, IndexType indexType)
    {
        using var store = GetDocumentStore();
        var collection = store.Conventions.FindCollectionName(typeof(TestObj));

        string schemaDefinition;
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            schemaDefinition =
                context.ReadObject(
                    new DynamicJsonValue { [SVC.Properties] = new DynamicJsonValue { [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [SVC.MaxLength] = 10 } } },
                    "schema-validation-configuration").ToString();
        }

        var schemaDefinitions = new IndexSchemaDefinitions { { collection, schemaDefinition } };
        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaDefinitions = schemaDefinitions,
            Fields = new Dictionary<string, IndexFieldOptions>{{"Error", new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        
        var e = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition)));
        Assert.Contains("Define a schema validation on metadata is not allowed", e.Message);
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaErrors_WhenSchemaDefinedInDatabase_ShouldIndexErrors(string map, IndexType indexType)
    {
        const string invalidDocId = "invalidDocId";
        const string validDocId = "validDocId";
        
        using var store = GetDocumentStore();
        var collection = store.Conventions.FindCollectionName(typeof(TestObj));
        
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
            Fields = new Dictionary<string, IndexFieldOptions>{{nameof(IndexResult.Errors), new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" }, invalidDocId);
            await session.StoreAsync(new TestObj { Prop = "01" }, validDocId);
            await session.SaveChangesAsync();
        }
        
        var configuration = new SchemaValidationConfiguration
        {
            ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
            {
                { collection, new SchemaDefinition { Schema = schemaDefinition } }
            }
        };
        await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));
        
        await store.Maintenance.SendAsync(new ResetIndexOperation(indexDefinition.Name));
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestObj>(indexDefinition.Name).ProjectInto<IndexResult>().ToArrayAsync();
            var dicResults = results.ToDictionary(o => o.Id);
            Assert.Null(dicResults[validDocId].Errors);

            var errors = dicResults[invalidDocId].Errors;
            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", errors.First());
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaErrors_WhenNoSchemaDefined_ShouldBeAlwaysValid(string map, IndexType indexType)
    {
        using var store = GetDocumentStore();
        
        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            Fields = new Dictionary<string, IndexFieldOptions>{{nameof(IndexResult.Errors), new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = "0123456789a" });
            await session.StoreAsync(new TestObj { Prop = "01" });
            await session.SaveChangesAsync();
        }
        
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenAsyncSession())
        {
            var results = await (from x in session.Query<TestObj>(indexDefinition.Name).As<IndexResult>() 
                let errors = x.Errors
                select new
                {
                    Errors = errors,
                }).ToArrayAsync();
            
            foreach (var r in results)
            {
                Assert.NotNull(r.Errors);
                Assert.Empty(r.Errors);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [MemberData(nameof(ValidateDocumentData))]
    public async Task IndexingSchemaErrors_WhenDefineOnEmptyCollection_ShouldGetTheError(string map, IndexType indexType)
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

        var schemaDefinitions = new IndexSchemaDefinitions { { Constants.Documents.Collections.EmptyCollection, schemaDefinition } };
        var indexDefinition = new IndexDefinition
        {
            Name = "IndexWithSchemaValidation",
            Maps = { map },
            SchemaDefinitions = schemaDefinitions,
            Fields = new Dictionary<string, IndexFieldOptions>{{nameof(IndexResult.Errors), new IndexFieldOptions{Storage = FieldStorage.Yes}}},
            Type = indexType
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new { Prop = "0123456789a" });
            await session.SaveChangesAsync();
        }
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<object>(indexDefinition.Name).ProjectInto<IndexResult>().ToArrayAsync();
            Assert.Single(results);
                
            var errors = results.Single().Errors;
            Assert.NotNull(errors);
            Assert.Single(errors);
            Assert.Contains("The length of the value '0123456789a' at 'Prop' should not exceed 10, but its actual length is 11.", errors.First());
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
        public string[] Errors { get; set; }
    }
}
