using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
                var q1 = session.Query<Dto>().VectorSearch(x => x.WithText("TextField").UsingTask("ai-task-identifier"), factory => factory.ByText("SomeText")).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.text(TextField,ai.task('ai-task-identifier')), $p0)", q1);

                var q2 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField").UsingTask("ai-task-identifier").TargetQuantization(VectorEmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.text_i8(TextField,ai.task('ai-task-identifier')), $p0)", q2);
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
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", ["apple"], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", ["computer"], dto2.Id);
            
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), factory => factory.ByText(queriedText), minimumSimilarity: 0.75f).ToList();

                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);

                var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
                var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);

                // we have to wait, since the savings of the embedding values for queries happens in async manner
                WaitForDocument<object>(store, valueEmbeddingsDocumentId, arg => true);

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
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [queriedText], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", ["computer"], dto2.Id);

            
            
            using (var session = store.OpenSession())
            {
                var result = session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), factory => factory.ByText(queriedText), minimumSimilarity: 0.75f).ToList();

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

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, 
                embeddingsPaths: new List<EmbeddingPathConfiguration>()
                {
                    new EmbeddingPathConfiguration()
                    {
                        ChunkingOptions = DefaultChunkingOptions,
                        Path = "TextualValue"
                    }
                });

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [queriedText], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", ["computer"], dto2.Id);
            
            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<InvalidQueryException>(() => session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue).UsingTask("NotExistingTask"), factory => factory.ByText(queriedText)).ToList());

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

            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto1.TextualValue], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto2.TextualValue], dto2.Id);
            
            
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
            var index = new SomeIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var result = session.Query<SomeIndex.IndexEntry, SomeIndex>().VectorSearch(x => x.WithField(d => d.TextualValueVector), factory => factory.ByText(queriedText), minimumSimilarity: 0.75f).ProjectInto<Dto>().ToList();

                Assert.Single(result);
                Assert.Equal(dto1.TextualValue, result[0].TextualValue);
            }

            var hash = EmbeddingsHelper.CalculateInputValueHash(queriedText);
            var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);

            Assert.True(WaitForDocument<object>(store, valueEmbeddingsDocumentId, predicate: null));
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
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto1.TextualValue], dto1.Id);
            
            var index = new SomeIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var result = session.Query<SomeIndex.IndexEntry, SomeIndex>().VectorSearch(x => x.WithField(d => d.TextualValueVector), factory => factory.ByText(queriedText), minimumSimilarity: 0.75f).ProjectInto<Dto>().ToList();

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
        var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { ChunkingOptions = DefaultChunkingOptions, Path = "TextualValue" }]);


        
        using (var session = store.OpenSession())
        {
            var dto1 = new Dto() { TextualValue = "pizza" };
            var dto2 = new Dto() { TextualValue = "car" };
            var dto3 = new Dto() { TextualValue = "beach" };
            
            session.Store(dto1);
            session.Store(dto2);
            session.Store(dto3);
            session.SaveChanges();

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto1.TextualValue], dto1.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto2.TextualValue], dto2.Id);
            AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto3.TextualValue], dto3.Id);
            
            
            var multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "vehicle"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "dog"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["cat", "dog"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["italian food", "vehicle"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["italian food", "dog"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.TextualValue), v => v.ByTexts(["cat", "dog"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task CanSearchByMultipleVectorsByTextsInParallel(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { ChunkingOptions = DefaultChunkingOptions, Path = "TextualValue" }]);

            using (var session1 = store.OpenAsyncSession())
            using (var session2 = store.OpenAsyncSession())
            using (var session3 = store.OpenAsyncSession())
            {
                var dto1 = new Dto() { TextualValue = "pizza" };
                var dto2 = new Dto() { TextualValue = "car" };
                var dto3 = new Dto() { TextualValue = "beach" };
                
                await session1.StoreAsync(dto1);
                await session1.StoreAsync(dto2);
                await session1.StoreAsync(dto3);
                await session1.SaveChangesAsync();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto1.TextualValue], dto1.Id);
                AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto2.TextualValue], dto2.Id);
                AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto3.TextualValue], dto3.Id);

                var q1 = session1.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "vehicle"]), minimumSimilarity: 0.75f)
                    .ToListAsync();

                var q2 = session2.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "dog"]), minimumSimilarity: 0.75f)
                    .ToListAsync();

                var q3 = session3.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["cat", "dog"]), minimumSimilarity: 0.75f).ToListAsync();

                Task.WaitAll(q1, q2, q3);

                Assert.Equal(2, q1.Result.Count);
                Assert.Equal(1, q2.Result.Count);
                Assert.Equal(0, q3.Result.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void WillFindUpdatedEmbeddingValues(Options options)
    {
        using var store = GetDocumentStore(options);

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { ChunkingOptions = DefaultChunkingOptions, Path = "TextualValue" }]);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { TextualValue = "asdsdfsdf" }, "dto/1");
            session.SaveChanges();

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, config, connection, "TextualValue", ["asdsdfsdf"], "dto/1");
            
            var multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "vehicle"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }

        aiTaskDone.Reset();

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { TextualValue = "pizza" }, "dto/1");
            session.SaveChanges();

            Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
            AssertEmbeddingsForPath(store, config, connection, "TextualValue", ["pizza"], "dto/1");

            var multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask("localaitask"), v => v.ByTexts(["italian food", "vehicle"]), minimumSimilarity: 0.75f).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);
        }
    }


    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MultiVectorSearchSumsDuplicates(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration(){ ChunkingOptions = DefaultChunkingOptions, Path = "TextualValue" }]);

            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { TextualValue = "pizza" };
                var dto2 = new Dto() { TextualValue = "fruit" };
                
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto1.TextualValue], dto1.Id);
                AssertEmbeddingsForPath(store, configuration, connectionString, "TextualValue", [dto2.TextualValue], dto2.Id);

                var multiVectorTextualQueryResult = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithText(s => s.TextualValue).UsingTask(configuration.Identifier), v => v.ByTexts(["pizza", "pineapple", "cherry", "strawberry", "blueberry"]))
                    .ToList();
                
                Assert.Equal(2, multiVectorTextualQueryResult.Count);
                Assert.Equal("fruit", multiVectorTextualQueryResult.First().TextualValue);
            }
        }
    }
    
    private class VectorStaticIndex : AbstractIndexCreationTask<Dto>
    {
        public VectorStaticIndex()
        {
            Map = dtos => dtos.Select(x => new
            {
                TextualValue = CreateVector(x.TextualValue),
                Vector = LoadVector("TextualValue", "localaitask")
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
                          select new IndexEntry { TextualValueVector = LoadVector("TextualValue", "localaitask") };
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string TextualValue { get; set; }
    }
}
