using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Indexing.Sorting;

namespace Raven.Database.Queries
{
	public class FacetedQueryRunner
	{
		private readonly DocumentDatabase database;

		public FacetedQueryRunner(DocumentDatabase database)
		{
			this.database = database;
		}

		public FacetResults GetFacets(string index, IndexQuery indexQuery, string facetSetupDoc)
		{
			var facetSetup = database.Get(facetSetupDoc, null);
			if (facetSetup == null)
				throw new InvalidOperationException("Could not find facets document: " + facetSetupDoc);

			var facets = facetSetup.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

			var results = new FacetResults();

			IndexSearcher currentIndexSearcher;
			using (database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				foreach (var facet in facets)
				{
					var facetResult = new FacetResult();

					switch (facet.Mode)
					{
						case FacetMode.Default:
							HandleTermsFacet(index, facet, indexQuery, currentIndexSearcher, facetResult);
							break;
						case FacetMode.Ranges:
							HandleRangeFacet(index, facet, indexQuery, currentIndexSearcher, facetResult);
							break;
						default:
							throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
					}

					results.Results[facet.Name] = facetResult;
				}

			}

			return results;
		}

		private void HandleRangeFacet(string index, Facet facet, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, FacetResult result)
		{
			foreach (var range in facet.Ranges)
			{
				var baseQuery = database.IndexStorage.GetLuceneQuery(index, indexQuery, database.IndexQueryTriggers);
				//TODO the built-in parser can't handle [NULL TO 100.0}, i.e. a mix of [ and }
				//so we need to handle this ourselves (greater and less-than-or-equal)
				var rangeQuery = database.IndexStorage.GetLuceneQuery(index, new IndexQuery
				{
					Query = facet.Name + ":" + range
				}, database.IndexQueryTriggers);

				var joinedQuery = new BooleanQuery();
				joinedQuery.Add(baseQuery, BooleanClause.Occur.MUST);
				joinedQuery.Add(rangeQuery, BooleanClause.Occur.MUST);

				var topDocs = currentIndexSearcher.Search(joinedQuery, null, 1);

				if (topDocs.TotalHits > 0)
				{
					result.Values.Add(new FacetValue
					{
						Count = topDocs.TotalHits,
						Range = range
					});
				}

				result.Terms.Add(range);
			}
		}

		private void HandleTermsFacet(string index, Facet facet, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, FacetResult result)
		{
			List<string> allTerms;
			var values = new List<FacetValue>();

			int maxResults = facet.MaxResults.GetValueOrDefault(database.Configuration.MaxPageSize);
			var baseQuery = database.IndexStorage.GetLuceneQuery(index, indexQuery, database.IndexQueryTriggers);

			var termCollector = new AllTermsCollector(facet.Name);
			currentIndexSearcher.Search(baseQuery, termCollector);

			if(facet.TermSortMode == FacetTermSortMode.ValueAsc)
				allTerms = new List<string>(termCollector.Groups.Keys.OrderBy((x) => x));
			else if(facet.TermSortMode == FacetTermSortMode.ValueDesc)
				allTerms = new List<string>(termCollector.Groups.Keys.OrderByDescending((x) => x));
			else if(facet.TermSortMode == FacetTermSortMode.HitsAsc)
				allTerms = new List<string>(termCollector.Groups.OrderBy((x) => x.Value).Select((x) => x.Key));
			else if(facet.TermSortMode == FacetTermSortMode.HitsDesc)
				allTerms = new List<string>(termCollector.Groups.OrderByDescending((x) => x.Value).Select((x) => x.Key));
			else
				throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));

			foreach(var term in allTerms)
			{
				if (values.Count >= maxResults)
					break;

				values.Add(new FacetValue
					           {
						           Count = termCollector.Groups[term],
								   Range = term
					           });
			}

			result.Values = values;
			result.Terms = allTerms;
		}

		private class AllTermsCollector : Collector
		{
			private readonly string field;
			private int currentDocBase;
			private string[] currentValues;
			private int[] currentOrders;
			private readonly Dictionary<string, int> groups = new Dictionary<string, int>();

			public AllTermsCollector(string field)
			{
				this.field = field;
			}

			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}

			public override void Collect(int doc)
			{
				string term = currentValues[currentOrders[doc]];
				if(!groups.ContainsKey(term))
					groups.Add(term, 1);
				else
					groups[term] = groups[term] + 1;
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				StringIndex currentReaderValues = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetStringIndex(reader, field);
				this.currentDocBase = docBase;
				this.currentOrders = currentReaderValues.order;
				this.currentValues = currentReaderValues.lookup;
			}

			public override void SetScorer(Scorer scorer)
			{
			}

			public IDictionary<string, int> Groups { get { return groups; } }
		}
	}
}