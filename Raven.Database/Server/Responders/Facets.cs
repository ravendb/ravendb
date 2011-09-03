using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Queries;
using Raven.Http.Abstractions;
using Raven.Database.Extensions;
using Raven.Http.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Facets : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/facets/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;
			
			var facets = context.GetFacetsFromHttpContext();
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);

			var results = new Dictionary<string, List<FacetValue>>();

			IndexSearcher currentIndexSearcher;
			using(Database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				foreach (var facet in facets)
				{
					switch (facet.Mode)
					{
						case FacetMode.Default:
							HandleTermsFacet(index, facet, indexQuery, currentIndexSearcher, results);
							break;
						case FacetMode.Ranges:
							HandleRangeFacet(index, facet, indexQuery, currentIndexSearcher, results);
							break;
						default:
							throw new ArgumentException("Could not understand " + facet.Mode);
					}
				}

			}

			context.WriteJson(results);
		}

		private void HandleRangeFacet(string index, Facet facet, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, Dictionary<string, List<FacetValue>> results)
		{
			var rangeResults = new List<FacetValue>();
			foreach (var range in facet.Ranges)
			{
				var baseQuery = Database.IndexStorage.GetLuceneQuery(index, indexQuery);
				var rangeQuery = Database.IndexStorage.GetLuceneQuery(index, new IndexQuery
				{
					Query = facet.Name + ":" + range
				});

				var joinedQuery = new BooleanQuery();
				joinedQuery.Add(baseQuery, BooleanClause.Occur.MUST);
				joinedQuery.Add(rangeQuery, BooleanClause.Occur.MUST);

				var topDocs = currentIndexSearcher.Search(joinedQuery, 1);

				if (topDocs.totalHits > 0)
					rangeResults.Add(new FacetValue
					{
						Count = topDocs.totalHits,
						Range = range
					});
			}
			results[facet.Name] = rangeResults;
		}

		private void HandleTermsFacet(string index, Facet facet, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, Dictionary<string, List<FacetValue>> results)
		{
			var terms = Database.ExecuteGetTermsQuery(index,
			                                          facet.Name,null,
			                                          Database.Configuration.MaxPageSize);
			var termResults = new List<FacetValue>();
			foreach (var term in terms)
			{
				var baseQuery = Database.IndexStorage.GetLuceneQuery(index, indexQuery);
				var termQuery = new TermQuery(new Term(facet.Name, term));

				var joinedQuery = new BooleanQuery();
				joinedQuery.Add(baseQuery, BooleanClause.Occur.MUST);
				joinedQuery.Add(termQuery, BooleanClause.Occur.MUST);

				var topDocs = currentIndexSearcher.Search(joinedQuery, 1);

				if(topDocs.totalHits > 0)
					termResults.Add(new FacetValue
					{
						Count = topDocs.totalHits,
						Range = term
					});
			}

			results[facet.Name] = termResults;
		}
	}

	public class FacetValue
	{
		public string Range { get; set; }
		public int Count { get; set; }
	}
}