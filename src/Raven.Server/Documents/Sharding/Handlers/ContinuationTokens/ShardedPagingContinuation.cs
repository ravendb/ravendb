using System.Collections.Generic;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;

public sealed class ShardedPagingContinuation : ContinuationToken
{
    public int PageSize;
    public Dictionary<int, ShardPaging> Pages;
    
    // if we asked to skip more than 'DeepPagingThreshold' items, we will approximate the paging
    private const int DeepPagingThreshold = 10_000;

    // Those properties are not sent back to the user
    // They used only for the very first request to figure out whether we should be precise on paging when 'start' param is passed
    // or if we in deep paging we will approximate the paging
    public int Skip;
    public bool DeepPaging;

    public ShardedPagingContinuation()
    {

    }

    public ShardedPagingContinuation(ShardedDatabaseContext databaseContext, int start, int pageSize)
    {
        var shards = databaseContext.ShardCount;

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Make this configurable?");
        DeepPaging = start * shards > DeepPagingThreshold;

        int startPortion = 0;
        int remaining = 0;
        
        if (DeepPaging)
        {
            startPortion = start / shards;
            remaining = start - startPortion * shards;
        }
        else
        {
            Skip = start;
        }
        
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
