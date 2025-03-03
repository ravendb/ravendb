using System;
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
        const string aiTaskName = "AiTaskName";
        const string connectionStringName = "ConnectionStringName";
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
            
            var configuration = new EmbeddingsGenerationConfiguration()
            {
                Name = aiTaskName,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }],
                Collection = "Dtos",
                Identifier = "ai-task-identifier",
                ChunkingOptionsForQuerying = DefaultChunkingOptions
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings(), Identifier = "connection-string-identifier" };

            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            
            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
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
        const string aiTaskName = "AiTaskName";
        const string connectionStringName = "ConnectionStringName";
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
            
            var configuration = new EmbeddingsGenerationConfiguration()
            {
                Name = aiTaskName,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }],
                Collection = "Dtos",
                ChunkingOptionsForQuerying = DefaultChunkingOptions
            };

            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };

            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, aiTaskName), factory => factory.ByText(queriedText)).ToList();
                
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
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<RavenException>(() => session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue, "NotExistingTask"), factory => factory.ByText(queriedText)).ToList());
                
                Assert.Contains("Couldn't find NotExistingTask AI task.", ex.Message);
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
            
            var etlDone = Etl.WaitForEtlToComplete(store);
            
            var (_, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            
            var index = new SomeIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<SomeIndex.IndexEntry, SomeIndex>().VectorSearch(x => x.WithField(d => d.TextualValueVector), factory => factory.ByText(queriedText)).ProjectInto<Dto>().ToList();
                
                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);
                
                WaitForUserToContinueTheTest(store);
                
                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);
                
                var valueEmbeddingsDocument = session.Load<object>(valueEmbeddingsDocumentId);

                Assert.NotNull(valueEmbeddingsDocument);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestChunkingInQuery(Options options)
    {
        const string aiTaskName = "AiTaskName";
        const string connectionStringName = "ConnectionStringName";
        const string queriedText = "computer machine technology tech";
        
        using (var store = GetDocumentStore(options))
        {
            var dto1 = new Dto() { TextualValue = "computer" };

            using (var session = store.OpenSession())
            {
                session.Store(dto1);
                session.SaveChanges();
            }
            
            var configuration = new EmbeddingsGenerationConfiguration()
            {
                Name = aiTaskName,
                AllowEtlOnNonEncryptedChannel = true,
                ConnectionStringName = connectionStringName,
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 5 }}],
                Collection = "Dtos",
                ChunkingOptionsForQuerying = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 5 }
            };
            
            var connectionString = new AiConnectionString() { Name = connectionStringName, OnnxSettings = new OnnxSettings() };
            
            var etlDone = Etl.WaitForEtlToComplete(store);

            Etl.AddEtl(store, configuration, connectionString);

            etlDone.Wait(TimeSpan.FromSeconds(10));

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
                select new IndexEntry { TextualValueVector = LoadVector("aitaskname", "TextualValue") };
        }
    }

    private class Dto
    {
        public string TextualValue { get; set; }
    }
}
