using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
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
    public void TestEmbeddingsGenerationForQuerying(Options options)
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
            
            var (configuration, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, configuration.Identifier), factory => factory.ByText(queriedText)).ToList();
                
                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);

                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);
                
                // todo wait for cacher
                //var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);

                //Assert.NotNull(valueEmbeddingsDocument);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestFetchingEmbeddingFromCache(Options options)
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

            var (configuration, connectionString) = RegisterAiIntegration(store,
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
    public void TestIfIncorrectTaskNameInQueryThrows(Options options)
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
            
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store);
            
            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<RavenException>(() => session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, "NotExistingTask"), factory => factory.ByText(queriedText)).ToList());
                
                Assert.Contains("Couldn't find NotExistingTask embeddings generation task.", ex.Message);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestEmbeddingGenerationWhenQueryingStaticIndex(Options options)
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
                
                // todo Michal wait for cacher
                /*
                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);
                
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);

                Assert.NotNull(valueEmbeddingsDocument);
                */
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestChunkingInQuery(Options options)
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
            
            var (configuration, connectionString) = RegisterAiIntegration(store, embeddingsPaths: [
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
