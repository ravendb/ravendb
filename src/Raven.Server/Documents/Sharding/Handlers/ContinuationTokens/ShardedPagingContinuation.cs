using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;

public class ShardedPagingContinuation : ContinuationToken
{
    public int PageSize;
    public ShardPaging[] Pages;

    public ShardedPagingContinuation()
    {

    }

    public ShardedPagingContinuation(ShardedDatabaseContext databaseContext, int start, int pageSize)
    {
        var shards = databaseContext.ShardCount;
        var startPortion = start / shards;
        var remaining = start - startPortion * shards;

        Pages = new ShardPaging[shards];

        for (var index = 0; index < Pages.Length; index++)
        {
            Pages[index].ShardNumber = index;
            Pages[index].Start = startPortion;
        }

        Pages[0].Start += remaining;

        PageSize = pageSize;
    }

    public override DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Pages)] = new DynamicJsonArray(Pages), 
            [nameof(PageSize)] = PageSize
        };
    }

    public struct ShardPaging : IDynamicJson
    {
        public int ShardNumber;
        public int Start;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Start)] = Start, 
                [nameof(ShardNumber)] = ShardNumber
            };
        }
    }
}
