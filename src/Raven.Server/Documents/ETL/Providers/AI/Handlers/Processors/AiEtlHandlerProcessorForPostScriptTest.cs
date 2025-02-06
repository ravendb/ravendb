using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForPostScriptTest : AbstractDatabaseEtlHandlerProcessorForTest<TestAiEtlScript, AiEtlConfiguration, AiConnectionString>
{
    public AiEtlHandlerProcessorForPostScriptTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestAiEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestAiEtlScript(json);
}
