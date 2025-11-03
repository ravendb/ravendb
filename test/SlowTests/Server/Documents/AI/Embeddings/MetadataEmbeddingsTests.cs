using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
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
    public class MetadataEmbeddingsTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
    {
        private class ProductDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public List<string> Categories { get; set; }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanGenerateEmbeddingsForMetadataFields()
        {
            using var store = GetDocumentStore();
            string docId;
            
            using (var session = store.OpenSession())
            {
                var product = new ProductDocument
                {
                    Name = "Smart Watch",
                    Description = "Advanced fitness tracking watch",
                    Categories = new List<string> { "Electronics", "Wearables" }
                };
                
                session.Store(product);
                docId = product.Id;
                
                // Add metadata that we want to embed
                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["brand"] = "TechCorp";
                metadata["marketingDescription"] = "Experience the future of fitness tracking with our revolutionary smartwatch";
                metadata["targetAudience"] = "Fitness enthusiasts and tech lovers";
                metadata["searchTags"] = JArray.FromObject(new[] { "smartwatch", "fitness", "health", "wearable" });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths:
            [
                new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "Description", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.brand", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.marketingDescription", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.targetAudience", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.searchTags", ChunkingOptions = DefaultChunkingOptions }
            ]);
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

            // Verify embeddings for document fields
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Smart Watch"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Description", ["Advanced fitness tracking watch"], docId);
            
            // Verify embeddings for metadata fields
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.brand", ["TechCorp"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.marketingDescription", ["Experience the future of fitness tracking with our revolutionary smartwatch"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.targetAudience", ["Fitness enthusiasts and tech lovers"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.searchTags", ["smartwatch", "fitness", "health", "wearable"], docId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanUpdateMetadataEmbeddings()
        {
            using var store = GetDocumentStore();
            string docId;
            
            using (var session = store.OpenSession())
            {
                var product = new ProductDocument
                {
                    Name = "Laptop",
                    Description = "High-performance laptop"
                };
                
                session.Store(product);
                docId = product.Id;
                
                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["category"] = "Computers";
                metadata["keywords"] = JArray.FromObject(new[] { "laptop", "computer" });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths:
            [
                new EmbeddingPathConfiguration() { Path = "@metadata.category", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.keywords", ChunkingOptions = DefaultChunkingOptions }
            ]);
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

            // Verify initial embeddings
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.category", ["Computers"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.keywords", ["laptop", "computer"], docId);

            // Update metadata
            aiTaskDone.Reset();
            using (var session = store.OpenSession())
            {
                var product = session.Load<ProductDocument>(docId);
                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["category"] = "Gaming Computers";
                metadata["keywords"] = JArray.FromObject(new[] { "gaming", "laptop", "high-performance" });
                
                session.SaveChanges();
            }

            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            // Verify updated embeddings
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.category", ["Gaming Computers"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.keywords", ["gaming", "laptop", "high-performance"], docId);
            
            // Verify old embeddings are removed
            AssertMissingEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.keywords", ["computer"], docId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanUseTransformationScriptWithMetadata()
        {
            using var store = GetDocumentStore();
            
            using (var session = store.OpenSession())
            {
                var product = new ProductDocument
                {
                    Name = "Electric Bike",
                    Description = "Eco-friendly transportation",
                    Categories = new List<string> { "Transportation", "Green" }
                };
                
                session.Store(product);
                
                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["manufacturer"] = "EcoBikes Inc";
                metadata["sustainabilityScore"] = 95;
                metadata["features"] = JArray.FromObject(new[] { "electric motor", "long battery life", "lightweight frame" });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
            // Use a transformation script that combines document and metadata fields
            var script = @"
                const metadata = this['@metadata'];
                const combined = {
                    ProductInfo: this.Name + ' by ' + metadata.manufacturer,
                    FullDescription: this.Description + '. Sustainability Score: ' + metadata.sustainabilityScore,
                    AllFeatures: metadata.features
                };
                
                embeddings.generate('ProductInfo', combined.ProductInfo);
                embeddings.generate('FullDescription', combined.FullDescription);
                
                for (let feature of combined.AllFeatures) {
                    embeddings.generate('AllFeatures', feature);
                }
            ";
            
            var (config, connection) = AddEmbeddingsGenerationTask(
                store, 
                script: script,
                collectionName: "ProductDocuments"
            );
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanHandleNestedMetadataStructures()
        {
            using var store = GetDocumentStore();
            string docId;
            
            using (var session = store.OpenSession())
            {
                var product = new ProductDocument
                {
                    Name = "Smart Home Hub",
                    Description = "Central control for your smart home"
                };
                
                session.Store(product);
                docId = product.Id;
                
                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["specifications"] = JObject.FromObject(new
                {
                    connectivity = new
                    {
                        wifi = "802.11ac",
                        bluetooth = "5.0",
                        protocols = new[] { "Zigbee", "Z-Wave", "Matter" }
                    },
                    compatibility = new
                    {
                        platforms = new[] { "iOS", "Android", "Web" },
                        voiceAssistants = new[] { "Alexa", "Google Assistant", "Siri" }
                    }
                });
                metadata["marketing"] = JObject.FromObject(new
                {
                    tagline = "Your home, smarter",
                    benefits = new[] { "Easy setup", "Universal compatibility", "Secure" }
                });
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths:
            [
                new EmbeddingPathConfiguration() { Path = "@metadata.specifications.connectivity.protocols", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.specifications.compatibility.voiceAssistants", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.marketing.tagline", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.marketing.benefits", ChunkingOptions = DefaultChunkingOptions }
            ]);
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

            // Verify embeddings for nested metadata
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, 
                "@metadata.specifications.connectivity.protocols", ["Zigbee", "Z-Wave", "Matter"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, 
                "@metadata.specifications.compatibility.voiceAssistants", ["Alexa", "Google Assistant", "Siri"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, 
                "@metadata.marketing.tagline", ["Your home, smarter"], docId);
            AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, 
                "@metadata.marketing.benefits", ["Easy setup", "Universal compatibility", "Secure"], docId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanMixDocumentFieldsAndMetadataInSingleTask()
        {
            using var store = GetDocumentStore();
            var docIds = new List<string>();
            
            using (var session = store.OpenSession())
            {
                // Create multiple documents with different metadata
                var products = new[]
                {
                    new ProductDocument
                    {
                        Name = "Wireless Headphones",
                        Description = "Premium audio experience",
                        Categories = new List<string> { "Audio", "Wireless" }
                    },
                    new ProductDocument
                    {
                        Name = "Bluetooth Speaker",
                        Description = "Portable sound system",
                        Categories = new List<string> { "Audio", "Portable" }
                    }
                };

                foreach (var product in products)
                {
                    session.Store(product);
                    docIds.Add(product.Id);
                    
                    var metadata = session.Advanced.GetMetadataFor(product);
                    metadata["audioQuality"] = "High-Resolution";
                    metadata["batteryLife"] = "20 hours";
                    metadata["specialFeatures"] = JArray.FromObject(new[] { "Noise cancellation", "Water resistant" });
                }
                
                session.SaveChanges();
            }

            var aiTaskDone = Etl.WaitForEtlToComplete(store);
            var (config, connection) = AddEmbeddingsGenerationTask(store, embeddingsPaths:
            [
                // Document fields
                new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "Description", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "Categories", ChunkingOptions = DefaultChunkingOptions },
                // Metadata fields
                new EmbeddingPathConfiguration() { Path = "@metadata.audioQuality", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.batteryLife", ChunkingOptions = DefaultChunkingOptions },
                new EmbeddingPathConfiguration() { Path = "@metadata.specialFeatures", ChunkingOptions = DefaultChunkingOptions }
            ]);
            
            Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
            var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

            // Verify embeddings for both documents
            foreach (var docId in docIds)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<ProductDocument>(docId);
                    
                    // Verify document field embeddings
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", [doc.Name], docId);
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Description", [doc.Description], docId);
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Categories", doc.Categories.ToArray(), docId);
                    
                    // Verify metadata embeddings
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.audioQuality", ["High-Resolution"], docId);
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.batteryLife", ["20 hours"], docId);
                    AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "@metadata.specialFeatures", ["Noise cancellation", "Water resistant"], docId);
                }
            }
        }
    }
}
