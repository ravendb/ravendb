using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors
{
    internal class EtlHandlerProcessorForTest : AbstractDatabaseEtlHandlerProcessorForTest<TestRavenEtlScript, RavenEtlConfiguration, RavenConnectionString>
    {
        public EtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestRavenEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRavenEtlScript(json);
    }
}
