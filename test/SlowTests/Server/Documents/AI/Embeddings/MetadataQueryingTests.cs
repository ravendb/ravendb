using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings
{
    public class MetadataQueryingTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
    {
        private class ResearchPaper
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Abstract { get; set; }
            public List<string> Authors { get; set; }
        }

        [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public async Task CanQueryByMetadataEmbeddings(Options options)
        {
            using var store = GetDocumentStore(options);
            
            var papers = new List<(ResearchPaper paper, Dictionary<string, object> metadata)>
            {
                (
                    new ResearchPaper
                    {
                        Title = "Neural Networks in Healthcare",
                        Abstract = "Application of deep learning in medical diagnosis",
                        Authors = new List<string> { "Dr. Smith", "Dr. Jones" }
                    },
                    new Dictionary<string, object>
                    {
                        ["field"] = "Medical AI",
                        ["keywords"] = new[] { "healthcare", "neural networks", "diagnosis" },
                        ["institution"] = "Medical Research Institute"
                    }
                ),
                (
                    new ResearchPaper
                    {
                        Title = "Quantum Computing Basics",
                        Abstract = "Introduction to quantum mechanics in computing",
                        Authors = new List<string> { "Prof. Anderson" }
                    },
                    new Dictionary<string, object>
                    {
                        ["field"] = "Quantum Physics",
                        ["keywords"] = new[] { "quantum", "computing", "physics" },
                        ["institution"] = "Physics Department University"
                    }
                ),
                (
                    new ResearchPaper
                    {
                        Title = "AI Ethics Framework",
                        Abstract = "Ethical considerations in artificial intelligence development",
                        Authors = new List<string> { "Dr. Williams" }
                    },
                    new Dictionary<string, object>
                    {
                        ["field"] = "AI Ethics",
                        ["keywords"] = new[] { "ethics", "AI", "framework", "responsibility" },
                        ["institution"] = "Ethics in Technology Center"
                    }
                )
            };

            // Store documents with metadata
            using (var session = store.OpenSession())
            {
                foreach (var (paper, metadataDict) in papers)
                {
                    session.Store(paper);
                    var metadata = session.Advanced.GetMetadataFor(paper);
                    
                    foreach (var kvp in metadataDict)
                    {
                        if (kvp.Value is string[] array)
                            metadata[kvp.Key] = JArray.FromObject(array);
                        else
                            metadata[kvp.Key] = kvp.Value.ToString();
                    }
                }
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, 
                embeddingsPaths:
                [
                    new EmbeddingPathConfiguration() { Path = "Title", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "Abstract", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.field", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.keywords", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.institution", ChunkingOptions = DefaultChunkingOptions }
                ],
                collectionName: "ResearchPapers"
            );
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                // Query by metadata field
                var medicalResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.field").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("Medical AI"), minimumSimilarity: 0.7f)
                    .ToList();
                
                Assert.Single(medicalResults);
                Assert.Contains("Healthcare", medicalResults[0].Title);

                // Query by metadata keywords
                var ethicsResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.keywords").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("ethical responsibility"), minimumSimilarity: 0.6f)
                    .ToList();
                
                Assert.Single(ethicsResults);
                Assert.Contains("Ethics", ethicsResults[0].Title);

                // Query by institution metadata
                var institutionResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.institution").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("Medical Institute"), minimumSimilarity: 0.7f)
                    .ToList();
                
                Assert.Single(institutionResults);
            }
        }

        [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public async Task CanCombineDocumentAndMetadataVectorQueries(Options options)
        {
            using var store = GetDocumentStore(options);
            
            using (var session = store.OpenSession())
            {
                var paper1 = new ResearchPaper
                {
                    Title = "Machine Learning in Finance",
                    Abstract = "Predictive models for financial markets"
                };
                session.Store(paper1);
                var metadata1 = session.Advanced.GetMetadataFor(paper1);
                metadata1["sector"] = "FinTech";
                metadata1["applicationArea"] = "Stock market prediction and risk assessment";

                var paper2 = new ResearchPaper
                {
                    Title = "Traditional Finance Models",
                    Abstract = "Classical approaches to market analysis"
                };
                session.Store(paper2);
                var metadata2 = session.Advanced.GetMetadataFor(paper2);
                metadata2["sector"] = "Traditional Finance";
                metadata2["applicationArea"] = "Historical market analysis methods";

                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, 
                embeddingsPaths:
                [
                    new EmbeddingPathConfiguration() { Path = "Title", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.sector", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.applicationArea", ChunkingOptions = DefaultChunkingOptions }
                ],
                collectionName: "ResearchPapers"
            );
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                // Query that should match based on title
                var titleResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText(d => d.Title).UsingTask(configuration.Identifier), 
                        factory => factory.ByText("AI in Financial Markets"), minimumSimilarity: 0.6f)
                    .ToList();
                
                Assert.Single(titleResults);
                Assert.Contains("Machine Learning", titleResults[0].Title);

                // Query that should match based on metadata
                var metadataResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.applicationArea").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("predict stock prices"), minimumSimilarity: 0.6f)
                    .ToList();
                
                Assert.Single(metadataResults);
                Assert.Contains("Machine Learning", metadataResults[0].Title);
            }
        }

        [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public async Task CanQueryNestedMetadataEmbeddings(Options options)
        {
            using var store = GetDocumentStore(options);
            
            using (var session = store.OpenSession())
            {
                var paper = new ResearchPaper
                {
                    Title = "Advanced NLP Techniques",
                    Abstract = "State-of-the-art natural language processing"
                };
                
                session.Store(paper);
                
                var metadata = session.Advanced.GetMetadataFor(paper);
                metadata["research"] = JObject.FromObject(new
                {
                    methodology = "Transformer-based architecture with attention mechanisms",
                    dataset = new
                    {
                        name = "Large Language Corpus",
                        description = "Multilingual text dataset with 100B tokens",
                        languages = new[] { "English", "Spanish", "French", "German" }
                    },
                    results = new
                    {
                        accuracy = 0.95,
                        summary = "Achieved state-of-the-art performance on multiple benchmarks"
                    }
                });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, 
                embeddingsPaths:
                [
                    new EmbeddingPathConfiguration() { Path = "@metadata.research.methodology", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.research.dataset.description", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.research.dataset.languages", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.research.results.summary", ChunkingOptions = DefaultChunkingOptions }
                ],
                collectionName: "ResearchPapers"
            );
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            using (var session = store.OpenSession())
            {
                // Query by nested methodology
                var methodologyResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.research.methodology").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("transformer attention"), minimumSimilarity: 0.7f)
                    .ToList();
                
                Assert.Single(methodologyResults);

                // Query by nested dataset description
                var datasetResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.research.dataset.description").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("multilingual corpus"), minimumSimilarity: 0.7f)
                    .ToList();
                
                Assert.Single(datasetResults);

                // Query by languages array
                var languageResults = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.research.dataset.languages").UsingTask(configuration.Identifier), 
                        factory => factory.ByText("Spanish French"), minimumSimilarity: 0.6f)
                    .ToList();
                
                Assert.Single(languageResults);
            }
        }

        [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public async Task MetadataEmbeddingsAreCached(Options options)
        {
            const string searchQuery = "innovative technology";
            
            using var store = GetDocumentStore(options);
            
            using (var session = store.OpenSession())
            {
                var paper = new ResearchPaper
                {
                    Title = "Tech Innovation Study",
                    Abstract = "Research on technological innovations"
                };
                
                session.Store(paper);
                
                var metadata = session.Advanced.GetMetadataFor(paper);
                metadata["researchFocus"] = searchQuery; // Use same text as query
                metadata["tags"] = JArray.FromObject(new[] { "innovation", "technology", "research" });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (configuration, connectionString) = AddEmbeddingsGenerationTask(store, 
                embeddingsPaths:
                [
                    new EmbeddingPathConfiguration() { Path = "@metadata.researchFocus", ChunkingOptions = DefaultChunkingOptions },
                    new EmbeddingPathConfiguration() { Path = "@metadata.tags", ChunkingOptions = DefaultChunkingOptions }
                ],
                collectionName: "ResearchPapers"
            );
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

            using (var session = store.OpenSession())
            {
                // First query - should generate embedding for query
                var results1 = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.researchFocus").UsingTask(configuration.Identifier), 
                        factory => factory.ByText(searchQuery), minimumSimilarity: 0.9f)
                    .ToList();
                
                Assert.Single(results1);

                // Verify query embedding was cached
                var hash = EmbeddingsHelper.CalculateInputValueHash(searchQuery);
                var cacheDocId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, hash, VectorEmbeddingType.Single);
                
                // Wait for cache document to be created
                WaitForDocument<object>(store, cacheDocId, arg => true);
                
                var cacheDoc = session.Load<object>(cacheDocId);
                Assert.NotNull(cacheDoc);

                // Second query with same text - should use cached embedding
                var results2 = session.Query<ResearchPaper>()
                    .VectorSearch(x => x.WithText("@metadata.researchFocus").UsingTask(configuration.Identifier), 
                        factory => factory.ByText(searchQuery), minimumSimilarity: 0.9f)
                    .ToList();
                
                Assert.Single(results2);
            }
        }

        private class MetadataVectorIndex : AbstractJavaScriptIndexCreationTask
        {
            public MetadataVectorIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('ResearchPapers', function (paper) {
                        var metadata = paper['@metadata'];
                        return {
                            Title: paper.Title,
                            CategoryVector: createVector(metadata.category),
                            TagsVector: metadata.tags ? createVector(metadata.tags.join(' ')) : null
                        };
                    })"
                };

                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "CategoryVector", new IndexFieldOptions { Vector = new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = VectorEmbeddingType.Single } } },
                    { "TagsVector", new IndexFieldOptions { Vector = new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = VectorEmbeddingType.Single } } }
                };
            }
        }

        [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CanQueryMetadataVectorsViaStaticIndex(Options options)
        {
            using var store = GetDocumentStore(options);
            
            using (var session = store.OpenSession())
            {
                var papers = new[]
                {
                    new ResearchPaper { Title = "AI Research Paper 1" },
                    new ResearchPaper { Title = "AI Research Paper 2" },
                    new ResearchPaper { Title = "Physics Paper" }
                };

                var categories = new[] { "Artificial Intelligence", "Machine Learning", "Quantum Physics" };
                var tags = new[]
                {
                    new[] { "AI", "neural networks", "deep learning" },
                    new[] { "AI", "algorithms", "optimization" },
                    new[] { "physics", "quantum", "theory" }
                };

                for (int i = 0; i < papers.Length; i++)
                {
                    session.Store(papers[i]);
                    var metadata = session.Advanced.GetMetadataFor(papers[i]);
                    metadata["category"] = categories[i];
                    metadata["tags"] = JArray.FromObject(tags[i]);
                }
                
                session.SaveChanges();
            }

            new MetadataVectorIndex().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                // Query by category vector
                var aiResults = session.Query<ResearchPaper, MetadataVectorIndex>()
                    .VectorSearch(f => f.WithField("CategoryVector"), v => v.ByText("AI and ML"))
                    .ToList();
                
                Assert.Equal(2, aiResults.Count);
                Assert.All(aiResults, r => Assert.Contains("AI", r.Title));

                // Query by tags vector
                var quantumResults = session.Query<ResearchPaper, MetadataVectorIndex>()
                    .VectorSearch(f => f.WithField("TagsVector"), v => v.ByText("quantum mechanics"))
                    .ToList();
                
                Assert.Single(quantumResults);
                Assert.Contains("Physics", quantumResults[0].Title);
            }
        }
    }
}
