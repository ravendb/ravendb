using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal class ShardedRavenEtlHandlerProcessorForTest : AbstractShardedEtlHandlerProcessorForTest<TestRavenEtlScript, RavenEtlConfiguration, RavenConnectionString>
    {
        public ShardedRavenEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestRavenEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRavenEtlScript(json);

        protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new RavenEtlTestCommand(json);
    }
}
