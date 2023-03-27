using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedAutoMapReduceIndexResultsAggregator : AutoMapReduceIndexResultsAggregator
{
    private static readonly DynamicJsonValue DummyDynamicJsonValue = new();

    public ShardedAutoMapReduceIndexResultsAggregator()
    {
        ModifyOutputToStore = djv =>
        {
            djv[Constants.Documents.Metadata.Key] = DummyDynamicJsonValue;
        };
    }
}
