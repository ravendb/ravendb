using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP.Handlers.Processors;

internal class OlapEtlHandlerProcessorForTest : AbstractDatabaseEtlHandlerProcessorForTest<TestOlapEtlScript, OlapEtlConfiguration, OlapConnectionString>
{
    public OlapEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestOlapEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestOlapEtlScript(json);
}
