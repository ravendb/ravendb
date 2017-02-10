using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;

namespace Raven.Client.Operations.Databases.Responses
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