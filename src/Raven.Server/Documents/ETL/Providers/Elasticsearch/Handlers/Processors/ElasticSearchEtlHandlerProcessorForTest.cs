using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;

internal class ElasticSearchEtlHandlerProcessorForTest : AbstractDatabaseEtlHandlerProcessorForTest<TestElasticSearchEtlScript, ElasticSearchEtlConfiguration, ElasticSearchConnectionString>
{
    public ElasticSearchEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestElasticSearchEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestElasticSearchEtlScript(json);
}
