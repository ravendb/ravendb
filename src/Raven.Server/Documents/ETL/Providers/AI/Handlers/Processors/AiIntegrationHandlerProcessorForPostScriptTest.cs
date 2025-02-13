using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiIntegrationHandlerProcessorForPostScriptTest : AbstractDatabaseEtlHandlerProcessorForTest<TestAiIntegrationScript, AiIntegrationConfiguration, AiConnectionString>
{
    public AiIntegrationHandlerProcessorForPostScriptTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestAiIntegrationScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestAiIntegrationScript(json);
}
