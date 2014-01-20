using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
#if !SILVERLIGHT
using Raven.Client.Connection;
using Raven.Client.Shard;
#endif
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
	public class LazyFacetsOperation : ILazyOperation
	{
		private readonly string index;
		private readonly IEnumerable<Facet> facets;
		private readonly string facetSetupDoc;
		private readonly IndexQuery query;
		private readonly int start;
		private readonly int? pageSize;

		public LazyFacetsOperation( string index, string facetSetupDoc, IndexQuery query, int start = 0, int? pageSize = null ) {
			this.index = index;
			this.facetSetupDoc = facetSetupDoc;
			this.query = query;
			this.start = start;
			this.pageSize = pageSize;
		}

		public LazyFacetsOperation(string index, IEnumerable<Facet> facets, IndexQuery query, int start = 0, int? pageSize = null)
		{
			this.index = index;
			this.facets = facets;
			this.query = query;
			this.start = start;
			this.pageSize = pageSize;
		}

		public GetRequest CreateRequest()
		{
			string addition;
			if (facetSetupDoc != null)
				addition = "facetDoc=" + facetSetupDoc;
			else
				addition = "facets=" + Uri.EscapeDataString(JsonConvert.SerializeObject(facets));

			return new GetRequest
			{
				Url = "/facets/" + index,
				Query = string.Format("{0}&facetStart={1}&facetPageSize={2}&{3}",
										query.GetMinimalQueryString(),
										start,
										pageSize,
										addition)
			};
		}

		public object Result { get; private set; }
		public QueryResult QueryResult { get; set; }
		public bool RequiresRetry { get; private set; }

		public void HandleResponse(GetResponse response)
		{
			if (response.Status != 200 && response.Status != 304)
			{
				throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" +
													response.Result);
			}

			var result = (RavenJObject)response.Result;
			Result = result.JsonDeserialization<FacetResults>();
		}

#if !SILVERLIGHT
		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var result = new FacetResults();

			foreach (var response in responses.Select(response => (RavenJObject)response.Result))
			{
				var facet = response.JsonDeserialization<FacetResults>();
				foreach (var facetResult in facet.Results)
				{
					if (!result.Results.ContainsKey(facetResult.Key))
						result.Results[facetResult.Key] = new FacetResult();

					var newFacetResult = result.Results[facetResult.Key];
					foreach (var facetValue in facetResult.Value.Values)
					{
						var existingFacetValueRange = newFacetResult.Values.Find((x) => x.Range == facetValue.Range);
						if (existingFacetValueRange != null)
							existingFacetValueRange.Hits += facetValue.Hits;
						else
							newFacetResult.Values.Add(new FacetValue() { Hits = facetValue.Hits, Range = facetValue.Range });
					}

					foreach (var facetTerm in facetResult.Value.RemainingTerms)
					{
						if (!newFacetResult.RemainingTerms.Contains(facetTerm))
							newFacetResult.RemainingTerms.Add(facetTerm);
					}
				}
			}

			Result = result;
		}
#endif

		public IDisposable EnterContext()
		{
			return null;
		}
#if !SILVERLIGHT
		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.GetFacets( index, query, facetSetupDoc, start, pageSize );
		}

		public void HandleEmbeddedResponse(object result)
		{
			Result = result;
		}
#endif
	}
}