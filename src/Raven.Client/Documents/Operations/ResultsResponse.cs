using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server.Operations.ApiKeys;

namespace Raven.Client.Documents.Operations
{
    public abstract class ResultsResponse<T>
    {
        public T[] Results { get; set; }
    }

    public class GetIndexNamesResponse : ResultsResponse<string>
    {
    }

    public class GetTransformerNamesResponse : ResultsResponse<string>
    {
    }

    public class PutIndexesResponse : ResultsResponse<PutIndexResult>
    {
    }

    public class GetIndexesResponse : ResultsResponse<IndexDefinition>
    {
    }

    public class GetTransformersResponse : ResultsResponse<TransformerDefinition>
    {
    }

    public class GetIndexStatisticsResponse : ResultsResponse<IndexStats>
    {
    }

    public class GetApiKeysResponse : ResultsResponse<NamedApiKeyDefinition>
    {
    }
}