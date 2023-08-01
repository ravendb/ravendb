using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;

public sealed class ShardedPagingContinuation : ContinuationToken
{
    public int PageSize;
    public Dictionary<int, ShardPaging> Pages;

    public ShardedPagingContinuation()
    {

    }

    public ShardedPagingContinuation(ShardedDatabaseContext databaseContext, int start, int pageSize)
    {
        var shards = databaseContext.ShardCount;
        var startPortion = start / shards;
        var remaining = start - startPortion * shards;

        Pages = new Dictionary<int, ShardPaging>(shards);

        foreach (var shardToTopology in databaseContext.ShardsTopology)
        {
            var page = new ShardPaging()
            {
                ShardNumber = shardToTopology.Key,
                Start = startPortion
            };

            if (remaining > 0)
            {
                page.Start++;
                remaining--;
            }

            Pages.Add(shardToTopology.Key, page);
        }

        PageSize = pageSize;
    }

    public override DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Pages)] = DynamicJsonValue.Convert(Pages), 
            [nameof(PageSize)] = PageSize
        };
    }

    public sealed class ShardPaging : IDynamicJson
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
