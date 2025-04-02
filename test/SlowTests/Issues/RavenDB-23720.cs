using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using SlowTests.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23720 : EmbeddingsGenerationTestBase
{
    public RavenDB_23720(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    public void AutoIndexWithVectorSearchShouldBeConvertibleToStatic()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Dto() { Name = "computer", EmbeddingRaw = [0.1f, 0.2f] });
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                var (configuration, _) = AddEmbeddingsGenerationTask(store);
                
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                
                var autoResults1 = session.Query<Dto>().VectorSearch(x => x.WithText(x => x.Name).UsingTask(configuration.Identifier),
                    factory => factory.ByText("some text")).ToList();
                
                var autoResults2 = session.Query<Dto>().Statistics(out var stats).VectorSearch(x => x.WithEmbedding(x => x.EmbeddingRaw),
                    factory => factory.ByEmbedding([0.1f, 0.2f])).ToList();
                
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

                var autoIndex = record.AutoIndexes.Values.First(x => x.Name == stats.IndexName);

                var staticDefinition = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

                var putIndexOp = new PutIndexesOperation(staticDefinition);
                
                store.Maintenance.Send(putIndexOp);
                
                Indexes.WaitForIndexing(store);

                var textFieldName = staticDefinition.Fields.Single(x => x.Value.Vector.SourceEmbeddingType == VectorEmbeddingType.Text).Key;
                var floatFieldName = staticDefinition.Fields.Single(x => x.Value.Vector.SourceEmbeddingType == VectorEmbeddingType.Single).Key;
                
                var staticResults1 = session.Query<Dto>(staticDefinition.Name).VectorSearch(x => x.WithField(textFieldName),
                    factory => factory.ByText("some text")).ToList();
                
                Assert.Equal(autoResults1, staticResults1);
                
                var staticResults2 = session.Query<Dto>(staticDefinition.Name).VectorSearch(x => x.WithField(floatFieldName),
                    factory => factory.ByEmbedding([0.1f, 0.2f])).ToList();
                
                Assert.Equal(autoResults2, staticResults2);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public float[] EmbeddingRaw { get; set; }
    }
}
