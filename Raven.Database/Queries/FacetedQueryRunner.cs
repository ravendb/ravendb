using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

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
			var rangeFacets = new List<List<ParsedRange>>();


			foreach (var facet in facets)
			{
				switch (facet.Mode)
				{
					case FacetMode.Default:
						//Remember the facet, so we can run them all under one query
						defaultFacets.Add(facet);
						results.Results[facet.Name] = new FacetResult();
						break;
					case FacetMode.Ranges:
						rangeFacets.Add(facet.Ranges.Select(range => ParseRange(facet.Name, range)).ToList());
						results.Results[facet.Name] = new FacetResult
						{
							Values = facet.Ranges.Select(range => new FacetValue
							{
								Range = range,
							}).ToList()
						};
					
						break;
					default:
						throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
				}
			}

			new QueryForFacets(database,index, defaultFacets, rangeFacets, indexQuery, results).Execute();

			return results;
			}

		private static ParsedRange ParseRange(string field, string range)
		{
			var parts = range.Split(new[] { " TO " }, 2, StringSplitOptions.RemoveEmptyEntries);

			if (parts.Length != 2)
				throw new ArgumentException("Could not understand range query: " + range);

			var trimmedLow = parts[0].Trim();
			var trimmedHigh = parts[1].Trim();
			var parsedRange = new ParsedRange
			{
				Field = field,
				RangeText = range,
				LowInclusive = IsInclusive(trimmedLow.First()),
				HighInclusive = IsInclusive(trimmedHigh.Last()),
				LowValue = trimmedLow.Substring(1),
				HighValue = trimmedHigh.Substring(0, trimmedHigh.Length - 1)
			};

			if (RangeQueryParser.NumerciRangeValue.IsMatch(parsedRange.LowValue))
			{
				parsedRange.LowValue = NumericStringToSortableNumeric(parsedRange.LowValue);
			}

			if (RangeQueryParser.NumerciRangeValue.IsMatch(parsedRange.HighValue))
			{
				parsedRange.HighValue = NumericStringToSortableNumeric(parsedRange.HighValue);
			}


			if (parsedRange.LowValue == "NULL" || parsedRange.LowValue == "*")
				parsedRange.LowValue = null;
			if (parsedRange.HighValue == "NULL" || parsedRange.HighValue == "*")
				parsedRange.HighValue = null;



			return parsedRange;
		}

		private static string NumericStringToSortableNumeric(string value)
		{
			var number = NumberUtil.StringToNumber(value);
			if (number is int)
			{
				return NumericUtils.IntToPrefixCoded((int)number);
			}
			if (number is long)
			{
				return NumericUtils.LongToPrefixCoded((long)number);
			}
			if (number is float)
			{
				return NumericUtils.FloatToPrefixCoded((float)number);
			}
			if (number is double)
			{
				return NumericUtils.DoubleToPrefixCoded((double)number);
			}

			throw new ArgumentException("Uknown type for " + number.GetType() + " which started as " + value);
		}

		private static bool IsInclusive(char ch)
		{
			switch (ch)
			{
				case '[':
				case ']':
					return true;
				case '{':
				case '}':
					return false;
				default:
					throw new ArgumentException("Could not understand range prefix: " + ch);
			}
		}

		private class ParsedRange
		{
			public bool LowInclusive;
			public bool HighInclusive;
			public string LowValue;
			public string HighValue;
			public string RangeText;
			public string Field;

			public bool IsMatch(string value)
			{
				var compareLow =
					LowValue == null
						? -1
						: string.CompareOrdinal(value, LowValue);
				var compareHigh = HighValue == null ? 1 : string.CompareOrdinal(value, HighValue);
				// if we are range exclusive on either end, check that we will skip the edge values
				if (compareLow == 0 && LowInclusive == false ||
					compareHigh == 0 && HighInclusive == false)
					return false;

				if (LowValue != null && compareLow < 0)
					return false;

				if (HighValue != null && compareHigh > 0)
					return false;

				return true;
			}
		}

		private class QueryForFacets
		{
			public QueryForFacets(
				DocumentDatabase database,
				string index,
				 List<Facet> facets,
				 List<List<ParsedRange>> ranges,
				 IndexQuery indexQuery,
				 FacetResults results)
		{
				Database = database;
				this.Index = index;
				this.Facets = facets;
				this.Ranges = ranges;
				this.IndexQuery = indexQuery;
				this.Results = results;
			}

			DocumentDatabase Database { get; set; }
			string Index { get; set; }
			List<Facet> Facets { get; set; }
			List<List<ParsedRange>> Ranges { get; set; }
			IndexQuery IndexQuery { get; set; }
			FacetResults Results { get; set; }

			public void Execute()
			{
				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				var allCollector = new GatherAllCollector();

				IndexSearcher currentIndexSearcher;
				RavenJObject[] termsDocs;
				using (Database.IndexStorage.GetCurrentIndexSearcherAndTermDocs(Index, out currentIndexSearcher, out termsDocs))
					{
					var baseQuery = Database.IndexStorage.GetLuceneQuery(Index, IndexQuery, Database.IndexQueryTriggers);
					currentIndexSearcher.Search(baseQuery, allCollector);
				}

				var facetsByName = GetFacetsByName(termsDocs, allCollector);
				
				UpdateFacetResults(facetsByName);
							}

			private void UpdateFacetResults(IDictionary<string, Dictionary<string, int>> facetsByName)
			{
				foreach (var facet in Facets)
			{
				var values = new List<FacetValue>();
				List<string> allTerms;

					int maxResults = Math.Min(facet.MaxResults ?? Database.Configuration.MaxPageSize, Database.Configuration.MaxPageSize);
				var groups = facetsByName.GetOrDefault(facet.Name);

					if (groups == null)
					continue;

				switch (facet.TermSortMode)
				{
					case FacetTermSortMode.ValueAsc:
						allTerms = new List<string>(groups.Keys.OrderBy(x => x));
						break;
					case FacetTermSortMode.ValueDesc:
						allTerms = new List<string>(groups.Keys.OrderByDescending(x => x));
						break;
					case FacetTermSortMode.HitsAsc:
						allTerms = new List<string>(groups.OrderBy(x => x.Value).Select(x => x.Key));
						break;
					case FacetTermSortMode.HitsDesc:
						allTerms = new List<string>(groups.OrderByDescending(x => x.Value).Select(x => x.Key));
						break;
					default:
						throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
				}

				foreach (var term in allTerms.TakeWhile(term => values.Count < maxResults))
				{
					values.Add(new FacetValue
					{
						Hits = groups.GetOrDefault(term),
						Range = term
					});
				}

					Results.Results[facet.Name] = new FacetResult
				{
					Values = values,
					RemainingTermsCount = allTerms.Count - values.Count,
					RemainingHits = groups.Values.Sum() - values.Sum(x => x.Hits),
				};

				if (facet.InclueRemainingTerms)
						Results.Results[facet.Name].RemainingTerms = allTerms.Skip(maxResults).ToList();
				}
			}

			private Dictionary<string, Dictionary<string, int>> GetFacetsByName(RavenJObject[] termsDocs, GatherAllCollector allCollector)
			{
				var facetsByName = new Dictionary<string, Dictionary<string, int>>();
				foreach (var docId in allCollector.Documents)
				{
					var doc = termsDocs[docId];
					foreach (var range in Ranges)
					{
						for (int i = 0; i < range.Count; i++)
						{
							var parsedRange = range[i];
							RavenJToken value;
							if (doc.TryGetValue(parsedRange.Field, out value) == false)
								continue;
							var facetResult = Results.Results[parsedRange.Field];

							switch (value.Type)
							{
								case JTokenType.String:
									if (parsedRange.IsMatch(value.Value<string>()))
										facetResult.Values[i].Hits++;
									break;
								case JTokenType.Array:
									var matches = value.Value<RavenJArray>().Count(item => parsedRange.IsMatch(item.Value<string>()));
									facetResult.Values[i].Hits += matches;
									break;
								default:
									throw new ArgumentException("Don't know how to deal with " + value.Type);
							}
						}
					}

					foreach (var facet in Facets)
					{
						RavenJToken value;
						if (doc.TryGetValue(facet.Name, out value) == false)
							continue;

						var facetValues = facetsByName.GetOrAdd(facet.Name);
						switch (value.Type)
						{
							case JTokenType.String:
								var term = value.Value<string>();
								facetValues[term] = facetValues.GetOrDefault(term) + 1;
								break;
							case JTokenType.Array:
								foreach (var item in value.Value<RavenJArray>())
								{
									var itemTerm = item.Value<string>();
									facetValues[itemTerm] = facetValues.GetOrDefault(itemTerm) + 1;
								}
								break;
							default:
								throw new ArgumentException("Don't know how to deal with " + value.Type);
						}
					}
				}
				return facetsByName;
			}
		}
	}


}
