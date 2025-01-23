using System.IO;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23556 : RavenTestBase
{
    public RavenDB_23556(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Test()
    {
        const string connectionStringName = "someConnectionStringName";

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto { Name = "very cool name" };
                session.Store(dto);
                session.SaveChanges();
            }

            var configuration = new VectorEmbeddingEnrichmentEtlConfiguration
            {
                Name = "someETLConfigurationName",
                ConnectionStringName = connectionStringName,
                Transforms = [new Transformation { Collections = ["Dtos"], Name = "CoolName", Script = "loadToWhatever(){}" }],
                FieldsToInclude = ["Name"],
                LlmProviderType = LlmProviderType.Onnx
            };

            // var connectionString = new AiConnectionString
            // {
            //     Name = connectionStringName,
            //     OllamaSettings = new OllamaSettings { Model = "mistral-nemo", Uri = "http://127.0.0.1:11434" }
            // };

            var connectionString = new AiConnectionString
            {
                Name = connectionStringName,
                OnnxSettings = new OnnxSettings
                {
                    ModelPath = Path.Combine("LocalEmbeddings", "bge-micro-v2", "model.onnx"),
                    VocabularyPath = Path.Combine("LocalEmbeddings", "bge-micro-v2", "vocab.txt"),
                    CaseSensitive = false
                }
            };

            Etl.AddEtl(store, configuration, connectionString);
            
            WaitForUserToContinueTheTest(store);
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}
