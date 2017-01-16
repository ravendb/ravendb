using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Data.Indexes;

namespace Raven.NewClient.Operations.Databases.Responses
{
    public abstract class ResultsResponse<T>
    {
        public T[] Results { get; set; }
    }

    public class GetIndexNamesResponse : ResultsResponse<string>
    {
    }

    public class GetIndexesResponse : ResultsResponse<IndexDefinition>
    {
    }

    public class GetIndexStatisticsResponse : ResultsResponse<IndexStats>
    {
    }
}