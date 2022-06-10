using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal class ShardedElasticSearchEtlHandlerProcessorForTest : AbstractShardedEtlHandlerProcessorForTest<TestElasticSearchEtlScript, ElasticSearchEtlConfiguration, ElasticSearchConnectionString>
{
    public ShardedElasticSearchEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestElasticSearchEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestElasticSearchEtlScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new ElasticSearchEtlTestCommand(json);
}
