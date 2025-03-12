using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class QueryingTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanGenerateRqlFromLinq(Options options)
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var q1 = session.Query<Dto>().VectorSearch(x => x.WithText("TextField", "EtlConfigName"), factory => factory.ByText("SomeText")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text(TextField,ai.task('EtlConfigName')), $p0)", q1);
                
                var q2 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField", "EtlConfigName").TargetQuantization(VectorEmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text_i8(TextField,ai.task('EtlConfigName')), $p0)", q2);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanGenerateEmbeddingsForQuerying(Options options)
    {
        const string queriedText = "fruit";
        
        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = "apple" };
            var dto2 = new Dto() { TextualValue = "computer" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, configuration.Identifier), factory => factory.ByText(queriedText)).ToList();
                
                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);

                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);

                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);

                Assert.NotNull(valueEmbeddingsDocument);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanFetchEmbeddingFromCache(Options options)
    {
        const string queriedText = "fruit";
        
        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = queriedText };
            var dto2 = new Dto() { TextualValue = "computer" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store,
                embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, configuration.Identifier), factory => factory.ByText(queriedText)).ToList();
                
                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DoesIncorrectTaskNameInQueryThrow(Options options)
    {
        const string queriedText = "fruit";

        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = queriedText };
            var dto2 = new Dto() { TextualValue = "computer" };

            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            AddEmbeddingsGenerationTask(store);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<InvalidQueryException>(() => session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, "NotExistingTask"), factory => factory.ByText(queriedText)).ToList());
                
                Assert.Contains("Couldn't find Embeddings Generation task with 'NotExistingTask' identifier", ex.Message);
                
                var indexCount = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length;
                
                Assert.Equal(0, indexCount);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanGenerateEmbeddingsWhenQueryingStaticIndex(Options options)
    {
        const string queriedText = "fruit";
        
        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = "apple" };
            var dto2 = new Dto() { TextualValue = "computer" };

            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            var (_, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            
            var index = new SomeIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<SomeIndex.IndexEntry, SomeIndex>().VectorSearch(x => x.WithField(d => d.TextualValueVector), factory => factory.ByText(queriedText)).ProjectInto<Dto>().ToList();
                
                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);
                
                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);

                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);

                Assert.NotNull(valueEmbeddingsDocument);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanChunkValueInQuery(Options options)
    {
        const string queriedText = "computer machine technology tech";
        
        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = "computer" };

            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.SaveChanges();
            }
            
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [
                new EmbeddingPathConfiguration()
                {
                    Path = "TextualValue", ChunkingOptions = new ChunkingOptions()
                    {
                        ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 5
                    }
                }
            ], chunkingOptionsForQuerying: new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 5 });
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            var index = new SomeIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<SomeIndex.IndexEntry, SomeIndex>().VectorSearch(x => x.WithField(d => d.TextualValueVector), factory => factory.ByText(queriedText)).ProjectInto<Dto>().ToList();

                Assert.Single(result);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanSearchByMultipleVectorsByTexts(Options options)
    {
        using var store = GetDocumentStore(options);

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration(){ ChunkingOptions = DefaultChunkingOptions, Path = "TextualValue" }]);
        
        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { TextualValue = "pizza" });
            session.Store(new Dto() { TextualValue = "car" });
            session.Store(new Dto() { TextualValue = "beach" });
            session.SaveChanges();

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            
            var multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue, embeddingsGenerationTaskIdentifier: "localaitask"), v => v.ByTexts(["italian food", "vehicle"])).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue, embeddingsGenerationTaskIdentifier: "localaitask"), v => v.ByTexts(["italian food", "dog"])).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue, embeddingsGenerationTaskIdentifier: "localaitask"), v => v.ByTexts(["cat", "dog"])).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["italian food", "vehicle"])).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["italian food", "dog"])).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["cat", "dog"])).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }
    }
    
    private class VectorStaticIndex : AbstractIndexCreationTask<Dto>
    {
        public VectorStaticIndex()
        {
            Map = dtos => dtos.Select(x => new
            {
                TextualValue = CreateVector(x.TextualValue),
                Vector = LoadVector("localaitask", "TextualValue")
            });
        }
    }
    
    private class SomeIndex : AbstractIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public object TextualValueVector { get; set; }
        }
        
        public SomeIndex()
        {
            Map = dtos => from dto in dtos
                select new IndexEntry { TextualValueVector = LoadVector("localaitask", "TextualValue") };
        }
    }

    private class Dto
    {
        public string TextualValue { get; set; }
    }
}
