using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23446(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    public async Task CanConvertVectorFieldToStaticField()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenAsyncSession())
        {
            await session.Query<Dto>()
                .VectorSearch(x => x.WithField(p => p.Vector), v => v.ByEmbedding([1f, 2f]))
                .ToListAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var autoIndex = record.AutoIndexes.Values.First();
        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
        RavenTestHelper.AssertEqualRespectingNewLines(@"public class Index_Dtos_ByVector_search_Vector_ : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Dto>
{
    public Index_Dtos_ByVector_search_Vector_()
    {
        Map = items => from item in items
                       select new
                       {
                           Vector = CreateVector(item.Vector)
                       };

        Vector(""Vector"", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
    }
}
", result);
        
        var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

        await store.Maintenance.SendAsync(new PutIndexesOperation(def));
        await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));

        using (var session = store.OpenAsyncSession())
        {
            var command = new RavenDB_22498.ConvertAutoIndexCommand(autoIndex.Name);
            await store.GetRequestExecutor().ExecuteAsync(command, session.Advanced.Context);

            var def2 = command.Result;
            
            // We accept that fields are different (since vector objects in AutoIndexes and Static indexes have different configuration objects, so we will check it manually).
            Assert.Equal(IndexDefinitionCompareDifferences.Fields, def.Compare(def2));
            Assert.Equal(1, def.Fields.Count);
            Assert.Equal(1, def2.Fields.Count);
            var convertField = def.Fields.First().Value.Vector;
            var autoField = def.Fields.First().Value.Vector;
            Assert.Equal(autoField.SourceEmbeddingType, convertField.SourceEmbeddingType);
            Assert.Equal(autoField.DestinationEmbeddingType, convertField.DestinationEmbeddingType);
            Assert.Equal(null, convertField.Dimensions);
            Assert.Equal(null, convertField.NumberOfEdges);
            Assert.Equal(null, convertField.NumberOfCandidatesForIndexing);
            Assert.Equal(null, autoField.Dimensions);
            Assert.Equal(null, autoField.NumberOfEdges);
            Assert.Equal(null, autoField.NumberOfCandidatesForIndexing);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    public async Task CanConvertVectorFieldWithQuantizationToStaticField()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenAsyncSession())
        {
            await session.Query<Dto>()
                .VectorSearch(x => x.WithEmbedding(p => p.Vector).TargetQuantization(VectorEmbeddingType.Int8), v => v.ByEmbedding([1f, 2f]))
                .ToListAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var autoIndex = record.AutoIndexes.Values.First();
        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
        RavenTestHelper.AssertEqualRespectingNewLines(@"public class Index_Dtos_ByVector_search_embedding_f32_i8_Vector__ : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Dto>
{
    public Index_Dtos_ByVector_search_embedding_f32_i8_Vector__()
    {
        Map = items => from item in items
                       select new
                       {
                           Vector = CreateVector(item.Vector)
                       };

        Vector(""Vector"", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Int8));
    }
}
", result);
        
        var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

        await store.Maintenance.SendAsync(new PutIndexesOperation(def));
        await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));

        using (var session = store.OpenAsyncSession())
        {
            var command = new RavenDB_22498.ConvertAutoIndexCommand(autoIndex.Name);
            await store.GetRequestExecutor().ExecuteAsync(command, session.Advanced.Context);

            var def2 = command.Result;
            
            // We accept that fields are different (since vector objects in AutoIndexes and Static indexes have different configuration objects, so we will check it manually).
            Assert.Equal(IndexDefinitionCompareDifferences.Fields, def.Compare(def2));
            Assert.Equal(1, def.Fields.Count);
            Assert.Equal(1, def2.Fields.Count);
            var convertField = def.Fields.First().Value.Vector;
            var autoField = def.Fields.First().Value.Vector;
            Assert.Equal(autoField.SourceEmbeddingType, convertField.SourceEmbeddingType);
            Assert.Equal(autoField.DestinationEmbeddingType, convertField.DestinationEmbeddingType);
            Assert.Equal(null, convertField.Dimensions);
            Assert.Equal(null, convertField.NumberOfEdges);
            Assert.Equal(null, convertField.NumberOfCandidatesForIndexing);
            Assert.Equal(null, autoField.Dimensions);
            Assert.Equal(null, autoField.NumberOfEdges);
            Assert.Equal(null, autoField.NumberOfCandidatesForIndexing);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    public async Task CanConvertTextualVectorFieldToStaticField()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenAsyncSession())
        {
            await session.Query<Dto>()
                .VectorSearch(x => x.WithText(p => p.Text), v => v.ByText("test"))
                .ToListAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var autoIndex = record.AutoIndexes.Values.First();
        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
        RavenTestHelper.AssertEqualRespectingNewLines(@"public class Index_Dtos_ByVector_search_embedding_text_Text__ : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Dto>
{
    public Index_Dtos_ByVector_search_embedding_text_Text__()
    {
        Map = items => from item in items
                       select new
                       {
                           Vector = CreateVector(item.Text)
                       };

        Vector(""Vector"", factory => factory.SourceEmbedding(VectorEmbeddingType.Text).DestinationEmbedding(VectorEmbeddingType.Single));
    }
}
", result);
        
        var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

        await store.Maintenance.SendAsync(new PutIndexesOperation(def));
        await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));

        using (var session = store.OpenAsyncSession())
        {
            var command = new RavenDB_22498.ConvertAutoIndexCommand(autoIndex.Name);
            await store.GetRequestExecutor().ExecuteAsync(command, session.Advanced.Context);

            var def2 = command.Result;
            
            // We accept that fields are different (since vector objects in AutoIndexes and Static indexes have different configuration objects, so we will check it manually).
            Assert.Equal(IndexDefinitionCompareDifferences.Fields, def.Compare(def2));
            Assert.Equal(1, def.Fields.Count);
            Assert.Equal(1, def2.Fields.Count);
            var convertField = def.Fields.First().Value.Vector;
            var autoField = def.Fields.First().Value.Vector;
            Assert.Equal(autoField.SourceEmbeddingType, convertField.SourceEmbeddingType);
            Assert.Equal(autoField.DestinationEmbeddingType, convertField.DestinationEmbeddingType);
            Assert.Equal(null, convertField.Dimensions);
            Assert.Equal(null, convertField.NumberOfEdges);
            Assert.Equal(null, convertField.NumberOfCandidatesForIndexing);
            Assert.Equal(null, autoField.Dimensions);
            Assert.Equal(null, autoField.NumberOfEdges);
            Assert.Equal(null, autoField.NumberOfCandidatesForIndexing);
        }
    }

    private class Dto
    {
        public string Text { get; set; }
        public float[] Vector { get; set; }
    }
}
