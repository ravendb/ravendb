using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class VectorChunkHighlightingTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    private const string MultiChunkText = "apple banana fruit\ncomputer technology machine";
    private const string SecondaryChunkText = "ocean water sea\nmountain rock stone";

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightReturnsMatchingChunkText(Options options)
    {
        using var store = GetDocumentStore(options);

        var dto = new Dto { TextualValue = MultiChunkText };
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);

            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);
            // the chunk nearest to "fruit" is the "apple banana fruit" line, not the "computer ..." one
            Assert.Contains("apple", fragments[0]);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task NoChunkTextWhenStoreChunkTextDisabled(Options options)
    {
        using var store = GetDocumentStore(options);

        var dto = new Dto { TextualValue = MultiChunkText };
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: false);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            // the query must still succeed and return the matching document; there is simply no chunk text to return
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);
            Assert.Empty(highlightings.GetFragments(results[0].Id));
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightWorksWithInt8Quantization(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true, quantization: VectorEmbeddingType.Int8);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier).TargetQuantization(VectorEmbeddingType.Int8), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);
            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);
            Assert.Contains("apple", fragments[0]);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightWorksWithMultipleQueryVectors(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByTexts(["fruit", "machine"]), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);
            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightResolvesEachFieldWithMultipleVectorSearches(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText, SecondaryValue = SecondaryChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateMultiFieldChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            // Two vector searches over two different fields on a dynamic (auto) index, each with its own highlight().
            // Each highlight must resolve to its own captured query vector - previously only a single vector search
            // per query could be highlighted (the field name was matched by cardinality, not by name).
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .AndAlso()
                .VectorSearch(f => f.WithText(d => d.SecondaryValue).UsingTask(configuration.Identifier), v => v.ByText("sea"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings textualHighlightings)
                .Highlight("SecondaryValue", 2048, 5, out Highlightings secondaryHighlightings)
                .ToList();

            Assert.NotEmpty(results);

            // each field is highlighted independently, against the query vector captured for that field
            var textualFragments = textualHighlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(textualFragments);
            Assert.Contains("apple", textualFragments[0]);

            var secondaryFragments = secondaryHighlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(secondaryFragments);
            Assert.Contains("ocean", secondaryFragments[0]);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightWorksOnStaticIndex(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        await new VectorViaLoadVectorIndex().ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Dto, VectorViaLoadVectorIndex>()
                .VectorSearch(f => f.WithField("TextualValueVector"), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValueVector", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);
            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);
            Assert.Contains("apple", fragments[0]);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DoesNotThrowWhenEmbeddingsDocumentMissing(Options options)
    {
        using var store = GetDocumentStore(options);

        string dtoId;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { TextualValue = MultiChunkText };
            session.Store(dto);
            session.SaveChanges();
            dtoId = dto.Id;
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            // get the document into the index, with its chunk text, before taking that chunk text away
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);
            Assert.NotEmpty(highlightings.GetFragments(results[0].Id));
        }

        // Deleting the embeddings document also drops the document from the vector index once indexing catches up, and a
        // query that returns nothing would not exercise the highlighter at all. Stopping indexing first keeps the index
        // entry in place while its chunk text source is gone - the state the highlighter has to survive.
        store.Maintenance.Send(new StopIndexingOperation());

        // remove the per-document embeddings document that holds the chunk text
        using (var session = store.OpenSession())
        {
            session.Delete("embeddings/" + dtoId);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            // the query must not throw even though the chunk text source is gone; it simply yields no fragments
            var results = session.Advanced.DocumentQuery<Dto>()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            // the document is still indexed, so the highlighter does run - it just has no chunk text to report
            Assert.NotEmpty(results);
            Assert.Empty(highlightings.GetFragments(results[0].Id));
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task IncludeHighlightMergesWithTermHighlightingOnSameField(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            // The same source field is both full-text searched ("banana") and vector searched ("fruit") under a single
            // highlight(). The term highlighter runs first and stores its tagged fragments; the vector chunk highlighter
            // must append its raw chunk fragments rather than overwrite them - both must survive.
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .Search("TextualValue", "banana")
                .AndAlso()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: 0.5f)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);

            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);

            // a term-highlight fragment (carries the closing markup tag) produced by the search("banana") clause
            Assert.Contains(fragments, f => f.Contains("</b>"));
            // a raw vector chunk fragment (no markup) produced by the vector search
            Assert.Contains(fragments, f => f.Contains("</b>") == false);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DoesNotReturnChunksBelowMinimumSimilarity(Options options)
    {
        // The document matches as soon as one of its chunks is near enough to the query, so the highlighter must apply
        // the query's minimumSimilarity itself - otherwise it would surface the document's unrelated chunks as well.
        // The threshold is derived from the model's own scores rather than hard-coded, so the test does not depend on
        // absolute similarity values: it sits between the near ("apple banana fruit") and far ("computer ...") chunk.
        float nearSimilarity = NormalizedSimilarity("fruit", "apple banana fruit");
        float farSimilarity = NormalizedSimilarity("fruit", "computer technology machine");
        Assert.True(farSimilarity < nearSimilarity, $"far ({farSimilarity}) < near ({nearSimilarity})");
        float minimumSimilarity = (nearSimilarity + farSimilarity) / 2f;

        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { TextualValue = MultiChunkText });
            session.SaveChanges();
        }

        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var configuration = CreateChunkingConfiguration(storeChunkText: true);
        AddEmbeddingsGenerationTask(store, configuration);

        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .VectorSearch(f => f.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), v => v.ByText("fruit"), minimumSimilarity: minimumSimilarity)
                .Highlight("TextualValue", 2048, 5, out Highlightings highlightings)
                .ToList();

            Assert.NotEmpty(results);

            var fragments = highlightings.GetFragments(results[0].Id);
            Assert.NotEmpty(fragments);
            Assert.Contains(fragments, f => f.Contains("apple"));
            Assert.DoesNotContain(fragments, f => f.Contains("computer"));
        }
    }

    // RavenDB reports vector similarity on a [0, 1] scale where 0.5 is orthogonal, i.e. (cosine + 1) / 2.
    private float NormalizedSimilarity(string queryText, string chunkText)
    {
        float[] query = GenerateEmbeddingForTextViaOnnx(queryText);
        float[] chunk = GenerateEmbeddingForTextViaOnnx(chunkText);
        Assert.Equal(query.Length, chunk.Length);

        float dot = 0f, querySquared = 0f, chunkSquared = 0f;
        for (int i = 0; i < query.Length; i++)
        {
            dot += query[i] * chunk[i];
            querySquared += query[i] * query[i];
            chunkSquared += chunk[i] * chunk[i];
        }

        float cosine = dot / (MathF.Sqrt(querySquared) * MathF.Sqrt(chunkSquared));
        return (cosine + 1f) / 2f;
    }

    private static EmbeddingsGenerationConfiguration CreateMultiFieldChunkingConfiguration(bool storeChunkText)
    {
        var chunking = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 8 };
        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = DefaultEmbeddingGenerationTaskName,
            ConnectionStringName = DefaultConnectionStringName,
            Collection = "Dtos",
            EmbeddingsPathConfigurations =
            [
                new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = chunking },
                new EmbeddingPathConfiguration { Path = "SecondaryValue", ChunkingOptions = chunking }
            ],
            ChunkingOptionsForQuerying = chunking,
            Quantization = VectorEmbeddingType.Single,
            StoreChunkText = storeChunkText
        };

        configuration.Identifier = configuration.GenerateIdentifier();
        return configuration;
    }

    private static EmbeddingsGenerationConfiguration CreateChunkingConfiguration(bool storeChunkText, VectorEmbeddingType quantization = VectorEmbeddingType.Single)
    {
        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = DefaultEmbeddingGenerationTaskName,
            ConnectionStringName = DefaultConnectionStringName,
            Collection = "Dtos",
            EmbeddingsPathConfigurations =
            [
                new EmbeddingPathConfiguration
                {
                    Path = "TextualValue",
                    ChunkingOptions = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 8 }
                }
            ],
            ChunkingOptionsForQuerying = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 8 },
            Quantization = quantization,
            StoreChunkText = storeChunkText
        };

        configuration.Identifier = configuration.GenerateIdentifier();
        return configuration;
    }

    private sealed class VectorViaLoadVectorIndex : AbstractIndexCreationTask<Dto>
    {
        public VectorViaLoadVectorIndex()
        {
            Map = dtos => from dto in dtos
                          select new { TextualValueVector = LoadVector("TextualValue", "localaitask") };
        }
    }

    private sealed class Dto
    {
        public string Id { get; set; }
        public string TextualValue { get; set; }
        public string SecondaryValue { get; set; }
    }
}
