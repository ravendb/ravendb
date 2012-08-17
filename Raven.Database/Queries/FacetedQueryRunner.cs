using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

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
			var defaultFacets = new List<Facet>();
			IndexSearcher currentIndexSearcher;

			using (database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				foreach (var facet in facets)
				{
					switch (facet.Mode)
					{
						case FacetMode.Default:
							//Remember the facet, so we can run them all under one query
							defaultFacets.Add(facet);
							break;
						case FacetMode.Ranges:
							var facetResult = new FacetResult();
							HandleRangeFacet(index, facet, indexQuery, currentIndexSearcher, facetResult);
							results.Results[facet.Name] = facetResult;
							break;
						default:
							throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
					}
				}

				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				if(defaultFacets.Count > 0)
					HandleTermsFacet(index, defaultFacets, indexQuery, currentIndexSearcher, results);
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
						Hits = topDocs.TotalHits,
						Range = range
					});
				}
			}
		}

		private void HandleTermsFacet(string index, List<Facet> facets, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, FacetResults results)
		{
			var baseQuery = database.IndexStorage.GetLuceneQuery(index, indexQuery, database.IndexQueryTriggers);
			var termCollector = new AllTermsCollector(facets.Select(x => x.Name));
			currentIndexSearcher.Search(baseQuery, termCollector);

			foreach(var facet in facets)
			{
				var values = new List<FacetValue>();
				List<string> allTerms;

				int maxResults = facet.MaxResults.GetValueOrDefault(database.Configuration.MaxPageSize);
				var groups = termCollector.GetGroupValues(facet.Name);

				switch (facet.TermSortMode)
				{
					case FacetTermSortMode.ValueAsc:
						allTerms = new List<string>(groups.Keys.OrderBy((x) => x));
						break;
					case FacetTermSortMode.ValueDesc:
						allTerms = new List<string>(groups.Keys.OrderByDescending((x) => x));
						break;
					case FacetTermSortMode.HitsAsc:
						allTerms = new List<string>(groups.OrderBy((x) => x.Value).Select((x) => x.Key));
						break;
					case FacetTermSortMode.HitsDesc:
						allTerms = new List<string>(groups.OrderByDescending((x) => x.Value).Select((x) => x.Key));
						break;
					default:
						throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
				}

				foreach (var term in allTerms)
				{
					if (values.Count >= maxResults)
						break;

					values.Add(new FacetValue
						           {
							           Hits = groups[term],
							           Range = term
						           });
				}

				results.Results[facet.Name] = new FacetResult()
					                              {
						                              Values = values,
						                              RemainingTermsCount = allTerms.Count - values.Count,
						                              RemainingHits = groups.Values.Sum() - values.Sum(x => x.Hits),
					                              };

				if(facet.InclueRemainingTerms)
					results.Results[facet.Name].RemainingTerms = allTerms.Skip(maxResults).ToList();
			}
		}

		private class AllTermsCollector : Collector
		{
			private readonly List<FieldData> fields = new List<FieldData>();

			public AllTermsCollector(IEnumerable<string> fields)
			{
				foreach(var field in fields)
					this.fields.Add(new FieldData { FieldName = field });
			}

			public IDictionary<string, int> GetGroupValues(string fieldName)
			{
				return fields.First(x => x.FieldName == fieldName).Groups;
			}

			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}

			public override void Collect(int doc)
			{
				//Since Collect can be called a rediculous number of times, we don't want to go through an iterator for this loop,
				//	thus no fancy foreach
				for(int i = 0; i < fields.Count; i++)
				{
					var data = fields[i];
					string term = data.CurrentValues[data.CurrentOrders[doc]];
					data.Groups[term] = data.Groups.GetOrAdd(term) + 1;
				}
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				foreach(var data in fields)
				{
					StringIndex currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, data.FieldName);
					data.CurrentOrders = currentReaderValues.order;
					data.CurrentValues = currentReaderValues.lookup;
				}
			}

			public override void SetScorer(Scorer scorer)
			{
			}

			private class FieldData
			{
				public string FieldName;
				public string[] CurrentValues;
				public int[] CurrentOrders;
				public readonly Dictionary<string, int> Groups = new Dictionary<string, int>();
			}
		}
	}
}