using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.AI;

public class AiEtlTests : RavenTestBase
{
    public AiEtlTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task CanTestAiEtlScript()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var order = new Order
                {
                    Lines =
                    [
                        new OrderLine { ProductName = "Carbon replacement feather for raven wing", Quantity = 450 },
                        new OrderLine { ProductName = "Plasma gun mount for raven's foot (left-side)", Quantity = 1 }
                    ]
                };

                await session.StoreAsync(order);
                await session.SaveChangesAsync();
            }

            var connectionString = new AiConnectionString { Name = "ConnectionStringForTestingPurposes", OnnxSettings = new OnnxSettings() };
            var operation = new PutConnectionStringOperation<AiConnectionString>(connectionString);
            var putConnectionStringResult = store.Maintenance.Send(operation);
            Assert.NotNull(putConnectionStringResult.RaftCommandIndex);

            var transformation = new Transformation
            {
                Name = "TransformationForTestingPurposes",
                Script = "loadToWhatever(){}",
                Collections = ["Orders"]
            };

            var aiEtlConfiguration = new AiEtlConfiguration
            {
                Name = "EtlForTestingPurposes",
                ConnectionStringName = "ConnectionStringForTestingPurposes",
                AiConnectorType = AiConnectorType.Onnx,
                PathsToProcess = ["Lines"],
                Transforms = [transformation],
            };

            var database = await GetDatabase(store.Database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testScript = new TestAiEtlScript
                {
                    DocumentId = "orders/1-A",
                    Configuration = aiEtlConfiguration
                };

                var testResult = AiEtl.TestScript(testScript, database, database.ServerStore, context);
                Assert.NotNull(testResult);
            }
        }
    }
}
