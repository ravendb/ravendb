using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Linq;
using System.Linq;
using Raven.Client.Data;

namespace Raven.Client.Shard
{
    public class ShardedRavenQueryInspector<T> : RavenQueryInspector<T>
    {
        private readonly ShardStrategy shardStrategy;
        private readonly List<IDatabaseCommands> shardDbCommands;
        private readonly List<IAsyncDatabaseCommands> asyncShardDbCommands;

        public ShardedRavenQueryInspector(
            ShardStrategy shardStrategy, 
            List<IDatabaseCommands> shardDbCommands,
             List<IAsyncDatabaseCommands> asyncShardDbCommands)
        {
            this.shardStrategy = shardStrategy;
            this.shardDbCommands = shardDbCommands;
            this.asyncShardDbCommands = asyncShardDbCommands;
        }

        public override FacetedQueryResult GetFacets(string facetSetupDoc, int start, int? pageSize)
        {
            var indexQuery = GetIndexQuery(false);
            var results = shardStrategy.ShardAccessStrategy.Apply(shardDbCommands, new ShardRequestData
            {
                IndexName = IndexQueried,
                EntityType = typeof (T),
                Query = indexQuery
            }, (commands, i) => commands.GetFacets(IndexQueried, indexQuery, facetSetupDoc, start, pageSize));

            return MergeFacets(results);
        }

        public override FacetedQueryResult GetFacets(List<Facet> facets, int start, int? pageSize)
        {
            var indexQuery = GetIndexQuery(false);
            var results = shardStrategy.ShardAccessStrategy.Apply(shardDbCommands, new ShardRequestData
            {
                IndexName = IndexQueried,
                EntityType = typeof(T),
                Query = indexQuery
            }, (commands, i) => commands.GetFacets(IndexQueried, indexQuery, facets, start, pageSize));

            return MergeFacets(results);
        }

        public override async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int start, int? pageSize, CancellationToken token = default (CancellationToken))
        {
            var indexQuery = GetIndexQuery(true);
            var results = await shardStrategy.ShardAccessStrategy.ApplyAsync(asyncShardDbCommands, new ShardRequestData
            {
                IndexName = AsyncIndexQueried,
                EntityType = typeof(T),
                Query = indexQuery
            }, (commands, i) => commands.GetFacetsAsync(AsyncIndexQueried, indexQuery, facets, start, pageSize, token)).ConfigureAwait(false);
        
            return MergeFacets(results);
        }

        public override async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int start, int? pageSize, CancellationToken token = default (CancellationToken))
        {
            var indexQuery = GetIndexQuery(true);
            var results = await shardStrategy.ShardAccessStrategy.ApplyAsync(asyncShardDbCommands, new ShardRequestData
            {
                IndexName = AsyncIndexQueried,
                EntityType = typeof(T),
                Query = indexQuery
            }, (commands, i) => commands.GetFacetsAsync(AsyncIndexQueried, indexQuery, facetSetupDoc, start, pageSize, token)).ConfigureAwait(false);

            return MergeFacets(results);
        }

        private FacetedQueryResult MergeFacets(FacetedQueryResult[] results)
        {
            if (results == null)
                return null;
            if (results.Length == 0)
                return null;
            if (results.Length == 1)
                return results[0];

            var finalResult = new FacetedQueryResult();

            var avgs = new Dictionary<FacetValue, List<double>>();

            foreach (var result in results.SelectMany(x=>x.Results))
            {
                FacetResult value;
                if (finalResult.Results.TryGetValue(result.Key, out value) == false)
                {
                    finalResult.Results[result.Key] = value = new FacetResult();
                }

                value.RemainingHits += result.Value.RemainingHits;
                if (result.Value.RemainingTerms != null && result.Value.RemainingTerms.Count > 0)
                {
                    value.RemainingTerms = value.RemainingTerms.Union(result.Value.RemainingTerms).ToList();
                }
                value.RemainingHits += result.Value.RemainingTermsCount;

                foreach (var facetValue in result.Value.Values)
                {
                    var match = value.Values.FirstOrDefault(x => x.Range == facetValue.Range);
                    if (match == null)
                    {
                        match = new FacetValue{Range = facetValue.Range};
                        value.Values.Add(facetValue);
                    }
                    
                    if(facetValue.Sum != null)
                        match.Sum += facetValue.Sum;
                    
                    if (match.Min != null || facetValue.Min != null)
                        match.Min = Math.Min(match.Min ?? double.MaxValue, facetValue.Min ?? double.MaxValue);

                    if (match.Max != null || facetValue.Max != null)
                        match.Max = Math.Min(match.Max ?? double.MinValue, facetValue.Max ?? double.MinValue);

                    match.Hits += facetValue.Hits;

                    if (facetValue.Count != null)
                        match.Count += facetValue.Count;

                    if (facetValue.Average != null)
                    {
                        List<double> list;
                        if (avgs.TryGetValue(match, out list) == false)
                        {
                            avgs[match] = list = new List<double>();
                        }
                        list.Add(facetValue.Average.Value);
                    }
                }
            }

            foreach (var avg in avgs)
            {
                avg.Key.Average = avg.Value.Average();
            }

            return finalResult;
        }
    }
}
