using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25306 : EmbeddingsGenerationTestBase
{
    public RavenDB_25306(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task EmbeddingsGenerationTaskShouldHandleReset()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Summary = "technical summary", Content = "technical content" };
                
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                var configuration = new EmbeddingsGenerationConfiguration
                {
                    Name = "ai-task-testing",
                    Identifier = "ai-task-testing",
                    ConnectionStringName = "ai-service-connection",
                    EmbeddingsPathConfigurations = [
                        new EmbeddingPathConfiguration() 
                        { 
                            Path = nameof(Dto.Content), 
                            ChunkingOptions = new ChunkingOptions()
                            {
                                ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs
                            }
                        }
                    ],
                    Collection = "Dtos",
                    ChunkingOptionsForQuerying = DefaultChunkingOptions
                };

                var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, EmbeddedSettings = new EmbeddedSettings() };
                connectionString.Identifier = connectionString.GenerateIdentifier();

                var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
                Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

                var addAiIntegrationTaskResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));
                
                Assert.NotNull(addAiIntegrationTaskResult.RaftCommandIndex);
                Assert.NotNull(addAiIntegrationTaskResult.TaskId);
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                
                configuration.EmbeddingsPathConfigurations =
                [
                    new EmbeddingPathConfiguration()
                    {
                        Path = nameof(Dto.Summary), 
                        ChunkingOptions = new ChunkingOptions()
                        {
                            ChunkingMethod = ChunkingMethod.PlainTextSplitParagraphs
                        }
                    }
                ];
                
                aiTaskDone.Reset();
                
                store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(addAiIntegrationTaskResult.TaskId, configuration, reset: true));
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                
                AssertEmbeddingsForPath(store, configuration, connectionString, nameof(Dto.Summary), [dto.Summary], dto.Id);
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Content { get; set; }
        public string Summary { get; set; }
    }
}
