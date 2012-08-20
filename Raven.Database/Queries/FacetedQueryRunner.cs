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
			var rangeFacets = new List<ParsedRange>();
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
							rangeFacets.AddRange(facet.Ranges.Select(range => ParseRange(facet.Name, range)));
							break;
						default:
							throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
					}
				}
				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
					
				QueryForFacets(index, defaultFacets, rangeFacets, indexQuery, currentIndexSearcher, results);
			}

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

				if(LowValue != null && compareLow < 0)
					return false;

				if(HighValue != null && compareHigh > 0)
					return false;

				return true;
			}
		}

		private void QueryForFacets(string index, List<Facet> facets, List<ParsedRange> ranges,IndexQuery indexQuery, IndexSearcher currentIndexSearcher, FacetResults results)
		{
			var baseQuery = database.IndexStorage.GetLuceneQuery(index, indexQuery, database.IndexQueryTriggers);
			var termCollector = new AllTermsCollector(facets.Select(x => x.Name), ranges);
			currentIndexSearcher.Search(baseQuery, termCollector);

			foreach (var range in ranges)
			{
				var facetResult = results.Results.GetOrAdd(range.Field);
				facetResult.Values.Add(new FacetValue
				{
					Range = range.RangeText,
					Hits = termCollector.GetRangeValue(range)
				});
			}
			foreach (var facet in facets)
			{
				var values = new List<FacetValue>();
				List<string> allTerms;

				int maxResults = Math.Min(facet.MaxResults ?? database.Configuration.MaxPageSize, database.Configuration.MaxPageSize);
				var groups = termCollector.GetGroupValues(facet.Name);

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

				results.Results[facet.Name] = new FacetResult
				{
					Values = values,
					RemainingTermsCount = allTerms.Count - values.Count,
					RemainingHits = groups.Values.Sum() - values.Sum(x => x.Hits),
				};

				if (facet.InclueRemainingTerms)
					results.Results[facet.Name].RemainingTerms = allTerms.Skip(maxResults).ToList();
			}
		}

		private class AllTermsCollector : Collector
		{
			private readonly List<FieldRangeData> ranges;
			private readonly List<FieldData> fields;

			public AllTermsCollector(IEnumerable<string> fields, IEnumerable<ParsedRange> ranges)
			{
				this.ranges = ranges.Select(x => new FieldRangeData {Range = x}).ToList();
				this.fields = fields.Select(field => new FieldData { FieldName = field }).ToList();
			}

			public IDictionary<string, int> GetGroupValues(string fieldName)
			{
				var firstOrDefault = fields.FirstOrDefault(x => x.FieldName == fieldName);
				if (firstOrDefault == null)
					return new Dictionary<string, int>();
				return firstOrDefault.Groups;
			}

			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}

			public override void Collect(int doc)
			{
				for (int index = 0; index < fields.Count; index++)
				{
					var data = fields[index];
					var term = data.CurrentValues[data.CurrentOrders[doc]];
					data.Groups[term] = data.Groups.GetOrAdd(term) + 1;
				}

				for (int index = 0; index < ranges.Count; index++)
				{
					var data = ranges[index];
					var term = data.CurrentValues[data.CurrentOrders[doc]];
					if (data.Range.IsMatch(term))
						data.Hits++;
				}
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				foreach (var data in fields)
				{
					var currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, data.FieldName);
					data.CurrentOrders = currentReaderValues.order;
					data.CurrentValues = currentReaderValues.lookup;
				}

				foreach (var data in ranges)
				{
					var currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, data.Range.Field);
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

			private class FieldRangeData
			{
				public ParsedRange Range;
				public string[] CurrentValues;
				public int[] CurrentOrders;
				public int Hits;
			}

			public int GetRangeValue(ParsedRange range)
			{
				var result = ranges.FirstOrDefault(x => x.Range == range);
				return result == null ? 0 : result.Hits;
			}
		}
	}
}
