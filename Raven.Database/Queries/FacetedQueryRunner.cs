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

			var defaultFacets = new List<Facet>();
			IndexSearcher currentIndexSearcher;
			using (database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				foreach (var facet in facets)
				{
					switch (facet.Mode)
					{
						case FacetMode.Default:
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
						Count = topDocs.TotalHits,
						Range = range
					});
				}

				result.Terms.Add(range);
			}
		}

		private void HandleTermsFacet(string index, IEnumerable<Facet> facets, IndexQuery indexQuery, IndexSearcher currentIndexSearcher, FacetResults results)
		{
			var values = new List<FacetValue>();

			var baseQuery = database.IndexStorage.GetLuceneQuery(index, indexQuery, database.IndexQueryTriggers);
			var termCollector = new AllTermsCollector(facets.Select(x => x.Name));
			currentIndexSearcher.Search(baseQuery, termCollector);

			foreach(var facet in facets)
			{
				List<string> allTerms;

				int maxResults = facet.MaxResults.GetValueOrDefault(database.Configuration.MaxPageSize);
				var groups = termCollector.GetGroups(facet.Name);

				if (facet.TermSortMode == FacetTermSortMode.ValueAsc)
					allTerms = new List<string>(groups.Keys.OrderBy((x) => x));
				else if (facet.TermSortMode == FacetTermSortMode.ValueDesc)
					allTerms = new List<string>(groups.Keys.OrderByDescending((x) => x));
				else if (facet.TermSortMode == FacetTermSortMode.HitsAsc)
					allTerms = new List<string>(groups.OrderBy((x) => x.Value).Select((x) => x.Key));
				else if (facet.TermSortMode == FacetTermSortMode.HitsDesc)
					allTerms = new List<string>(groups.OrderByDescending((x) => x.Value).Select((x) => x.Key));
				else
					throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));

				foreach (var term in allTerms)
				{
					if (values.Count >= maxResults)
						break;

					values.Add(new FacetValue
						           {
							           Count = groups[term],
							           Range = term
						           });
				}

				results.Results[facet.Name] = new FacetResult() {Terms = allTerms, Values = values};
			}
		}

		private class AllTermsCollector : Collector
		{
			private int currentDocBase;
			private readonly List<FieldData> fields = new List<FieldData>();

			public AllTermsCollector(IEnumerable<string> fields)
			{
				foreach(var field in fields)
					this.fields.Add(new FieldData { FieldName = field });
			}

			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}

			public override void Collect(int doc)
			{
				foreach (var data in fields)
				{
					string term = data.CurrentValues[data.CurrentOrders[doc]];
					if (!data.Groups.ContainsKey(term))
						data.Groups.Add(term, 1);
					else
						data.Groups[term] = data.Groups[term] + 1;
				}
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				this.currentDocBase = docBase;

				foreach(var data in fields)
				{
					StringIndex currentReaderValues = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetStringIndex(reader, data.FieldName);
					data.CurrentOrders = currentReaderValues.order;
					data.CurrentValues = currentReaderValues.lookup;
				}
			}

			public override void SetScorer(Scorer scorer)
			{
			}

			public IDictionary<string, int> GetGroups(string fieldName)
			{
				return fields.Where(x => x.FieldName == fieldName).First().Groups;
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