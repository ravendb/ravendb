#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Client.Shard
{
	public class ShardedRavenQueryInspector<T> : RavenQueryInspector<T>
	{
		private readonly ShardStrategy shardStrategy;
		private readonly List<IDatabaseCommands> shardDbCommands;
		private readonly List<IAsyncDatabaseCommands> asyncShardDbCommands;

		public ShardedRavenQueryInspector(IRavenQueryProvider provider,
		 RavenQueryStatistics queryStats, RavenQueryHighlightings highlightings, string indexName, Expression expression, InMemoryDocumentSessionOperations session, bool isMapReduce,
			ShardStrategy shardStrategy, 
			List<IDatabaseCommands> shardDbCommands,
			 List<IAsyncDatabaseCommands> asyncShardDbCommands)
			: base(provider, queryStats, highlightings, indexName, expression, session,
			 null,
			 null, 
			 isMapReduce)
		{
			this.shardStrategy = shardStrategy;
			this.shardDbCommands = shardDbCommands;
			this.asyncShardDbCommands = asyncShardDbCommands;
		}

		public override Abstractions.Data.FacetResults GetFacets(string facetSetupDoc, int start, int? pageSize)
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

		public override Abstractions.Data.FacetResults GetFacets(List<Abstractions.Data.Facet> facets, int start, int? pageSize)
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

		public override async System.Threading.Tasks.Task<Abstractions.Data.FacetResults> GetFacetsAsync(List<Abstractions.Data.Facet> facets, int start, int? pageSize)
		{
			var indexQuery = GetIndexQuery(false);
			var results = await shardStrategy.ShardAccessStrategy.ApplyAsync(asyncShardDbCommands, new ShardRequestData
			{
				IndexName = IndexQueried,
				EntityType = typeof(T),
				Query = indexQuery
            }, (commands, i) => commands.GetFacetsAsync(IndexQueried, indexQuery, facets, start, pageSize)).ConfigureAwait(false);

			return MergeFacets(results);
		}

		public override async System.Threading.Tasks.Task<Abstractions.Data.FacetResults> GetFacetsAsync(string facetSetupDoc, int start, int? pageSize)
		{
			var indexQuery = GetIndexQuery(false);
			var results = await shardStrategy.ShardAccessStrategy.ApplyAsync(asyncShardDbCommands, new ShardRequestData
			{
				IndexName = IndexQueried,
				EntityType = typeof(T),
				Query = indexQuery
            }, (commands, i) => commands.GetFacetsAsync(IndexQueried, indexQuery, facetSetupDoc, start, pageSize)).ConfigureAwait(false);

			return MergeFacets(results);
		}

		private FacetResults MergeFacets(FacetResults[] results)
		{
			if (results == null)
				return null;
			if (results.Length == 0)
				return null;
			if (results.Length == 1)
				return results[0];

			var finalResult = new FacetResults();

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
#endif
