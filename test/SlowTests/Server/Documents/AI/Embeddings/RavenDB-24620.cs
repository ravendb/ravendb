using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_24620(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying, RavenArchitecture.AllX64)]
    [InlineData(null, null)]
    [InlineData(null, VectorEmbeddingType.Single)]
    [InlineData(null, VectorEmbeddingType.Int8)]
    [InlineData(null, VectorEmbeddingType.Binary)]
    [InlineData(VectorEmbeddingType.Single, VectorEmbeddingType.Single)]
    [InlineData(VectorEmbeddingType.Single, VectorEmbeddingType.Int8)]
    [InlineData(VectorEmbeddingType.Single, VectorEmbeddingType.Binary)]
    [InlineData(VectorEmbeddingType.Int8, VectorEmbeddingType.Int8)]
    [InlineData(VectorEmbeddingType.Binary, VectorEmbeddingType.Binary)]
    public async Task CanUseTaskToQueryPregeneratedEmbedding(VectorEmbeddingType? source, VectorEmbeddingType? destination)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        PrepareDocumentsWithAttachments(store, source);
        var aiTaskDone = Etl.WaitForEtlToComplete(store);
        var (aiIntegrationConfiguration, aiConnectionString) = AddEmbeddingsGenerationTask(store, targetQuantization: source ?? VectorEmbeddingType.Single);
        Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));

        var index = new VectorIndex(source, destination);
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var rqlResult = session.Query<Dto, VectorIndex>().VectorSearch(
                f => f.WithField(s => s.Vector),
                v => v.ByText("car", aiIntegrationConfiguration.Identifier)).ToString();
            Assert.Equal("from index 'VectorIndex' where vector.search(Vector, embedding.text($p0, ai.task('localaitask')))", rqlResult);

            var results = session.Query<Dto, VectorIndex>().VectorSearch(
                    f => f.WithField(s => s.Vector),
                    v => v.ByText("animal", aiIntegrationConfiguration.Identifier))
                .ToList();

            Assert.Equal(results[0].HelpName, "dog");


            results = session.Advanced.DocumentQuery<Dto, VectorIndex>()
                .VectorSearch(
                    f => f.WithField(s => s.Vector),
                    v => v.ByText("animal", aiIntegrationConfiguration.Identifier))
                .ToList();
            Assert.Equal(results[0].HelpName, "dog");
        }
        
        using (var session = store.OpenSession())
        {
            var rqlResult = session.Query<Dto, VectorIndex>().VectorSearch(
                f => f.WithField(s => s.Vector),
                v => v.ByTexts(["car", "planet"], aiIntegrationConfiguration.Identifier)).ToString();
            Assert.Equal("from index 'VectorIndex' where vector.search(Vector, embedding.text($p0, ai.task('localaitask')))", rqlResult);

            var results = session.Query<Dto, VectorIndex>().VectorSearch(
                    f => f.WithField(s => s.Vector),
                    v => v.ByTexts(["car", "cosmos"], aiIntegrationConfiguration.Identifier))
                .ToList();

            Assert.True(new[]{"car", "sun"}.Contains(results[0].HelpName));
            Assert.True(new[]{"car", "sun"}.Contains(results[1].HelpName));


            results = session.Advanced.DocumentQuery<Dto, VectorIndex>()
                .VectorSearch(
                    f => f.WithField(s => s.Vector),
                    v => v.ByTexts(["car", "cosmos"], aiIntegrationConfiguration.Identifier))
                .ToList();
            Assert.True(new[]{"car", "sun"}.Contains(results[0].HelpName));
            Assert.True(new[]{"car", "sun"}.Contains(results[1].HelpName));
        }
    }

    private static void PrepareDocumentsWithAttachments(IDocumentStore store, VectorEmbeddingType? embeddingType)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var session = store.OpenSession();
        Dto[] dtos = [new() { HelpName = "car" }, new() { HelpName = "dog" }, new() { HelpName = "sun" }];
        session.Store(dtos[0]);
        session.Store(dtos[1]);
        session.Store(dtos[2]);
        session.SaveChanges();

        var config = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = embeddingType ?? VectorEmbeddingType.Single };

        using var car = Raven.Server.Documents.Indexes.VectorSearch.GenerateEmbeddings.FromText(bsc, config, "car");
        using var dog = Raven.Server.Documents.Indexes.VectorSearch.GenerateEmbeddings.FromText(bsc, config, "dog");
        using var sun = Raven.Server.Documents.Indexes.VectorSearch.GenerateEmbeddings.FromText(bsc, config, "sun");

        session.Advanced.Attachments.Store(dtos[0].Id, "vector", new MemoryStream(car.GetEmbedding().ToArray()));
        session.Advanced.Attachments.Store(dtos[1].Id, "vector", new MemoryStream(dog.GetEmbedding().ToArray()));
        session.Advanced.Attachments.Store(dtos[2].Id, "vector", new MemoryStream(sun.GetEmbedding().ToArray()));
        session.SaveChanges();
    }

    private class Dto
    {
        public string Id { get; set; }
        public string HelpName { get; set; }
        public object Vector { get; set; }
    }

    private class VectorIndex : AbstractIndexCreationTask
    {
        private readonly VectorEmbeddingType? _source;
        private readonly VectorEmbeddingType? _destination;

        public VectorIndex()
        {
            // for querying
        }
        
        public VectorIndex(VectorEmbeddingType? source, VectorEmbeddingType? destination)
        {
            _source = source;
            _destination = destination;
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            var indexDefinition = new IndexDefinition()
            {
                Maps =
                [
                    @"from dto in docs.Dtos
                let attachment = LoadAttachment(dto, ""vector"")
                select new { Vector =  CreateVector(attachment.GetContentAsStream())}"
                ]
            };

            if (_source != null || _destination != null)
            {
                var vecOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = _destination ?? VectorEmbeddingType.Single, SourceEmbeddingType = _source ?? VectorEmbeddingType.Single,
                };

                indexDefinition.Fields = new Dictionary<string, IndexFieldOptions>();
                indexDefinition.Fields.Add("Vector", new IndexFieldOptions() { Vector = vecOptions });
            }

            return indexDefinition;
        }
    }
}
