using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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

		public FacetResults GetFacets(string index, IndexQuery indexQuery, List<Facet> facets, int start = 0, int? pageSize = null)
		{
			var results = new FacetResults();
			var defaultFacets = new Dictionary<string, Facet>();
			var rangeFacets = new Dictionary<string, List<ParsedRange>>();

			foreach (var facet in facets)
			{
                defaultFacets[facet.Name] = facet;
                if (facet.Aggregation != FacetAggregation.Count)
                {
                    if (string.IsNullOrEmpty(facet.AggregationField))
                        throw new InvalidOperationException("Facet " + facet.Name + " cannot have aggregation set to " +
                                                            facet.Aggregation + " without having a value in AggregationField");

                    if (facet.AggregationField.EndsWith("_Range") == false)
                        facet.AggregationField = facet.AggregationField + "_Range";
                }
                switch (facet.Mode)
				{
					case FacetMode.Default:
						results.Results[facet.Name] = new FacetResult();
				        break;
					case FacetMode.Ranges:
						rangeFacets[facet.Name] = facet.Ranges.Select(range => ParseRange(facet.Name, range)).ToList();
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

			var queryForFacets = new QueryForFacets(database, index, defaultFacets, rangeFacets, indexQuery, results, start, pageSize);
			queryForFacets.Execute();

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

			if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.LowValue))
			{
				parsedRange.LowValue = NumericStringToSortableNumeric(parsedRange.LowValue);
			}

			if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.HighValue))
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

			throw new ArgumentException("Unknown type for " + number.GetType() + " which started as " + value);
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

			public override string ToString()
			{
				return string.Format("{0}:{1}", Field, RangeText);
			}
		}

		private class QueryForFacets
		{
		    public QueryForFacets(
				DocumentDatabase database,
				string index,
				 Dictionary<string, Facet> facets,
				 Dictionary<string, List<ParsedRange>> ranges,
				 IndexQuery indexQuery,
				 FacetResults results,
				 int start,
				 int? pageSize)
			{
				Database = database;
				Index = index;
				Facets = facets;
				Ranges = ranges;
				IndexQuery = indexQuery;
				Results = results;
				Start = start;
				PageSize = pageSize;
			}

			DocumentDatabase Database { get; set; }
			string Index { get; set; }
			Dictionary<string, Facet> Facets { get; set; }
			Dictionary<string, List<ParsedRange>> Ranges { get; set; }
			IndexQuery IndexQuery { get; set; }
			FacetResults Results { get; set; }
			private int Start { get; set; }
			private int? PageSize { get; set; }

			public void Execute()
			{
				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				var allCollector = new GatherAllCollector();
                var facetsByName = new Dictionary<string, Dictionary<string, FacetValue>>();

				IndexSearcher currentIndexSearcher;
				using (Database.IndexStorage.GetCurrentIndexSearcher(Index, out currentIndexSearcher))
				{
					var baseQuery = Database.IndexStorage.GetLuceneQuery(Index, IndexQuery, Database.IndexQueryTriggers);
					currentIndexSearcher.Search(baseQuery, allCollector);
					var fields = Facets.Values.Select(x => x.Name)
							.Concat(Ranges.Select(x => x.Key));
					var fieldsToRead = new HashSet<string>(fields);
					IndexedTerms.ReadEntriesForFields(currentIndexSearcher.IndexReader,
						fieldsToRead,
						allCollector.Documents,
						(term, doc) =>
						{
                            Facet value;
						    if (Facets.TryGetValue(term.Field, out value) == false)
						        return;

						    switch (value.Mode)
						    {
						        case FacetMode.Default:
                                    var facetValues = facetsByName.GetOrAdd(term.Field);
						            FacetValue existing;
						            if (facetValues.TryGetValue(term.Text, out existing) == false)
						            {
						                existing = new FacetValue{Range = term.Text};
						                facetValues[term.Text] = existing;
						            }
						            UpdateValue(existing, value, doc, currentIndexSearcher.IndexReader);
						            break;
						        case FacetMode.Ranges:
                                    List<ParsedRange> list;
							        if (Ranges.TryGetValue(term.Field, out list))
							        {
								        for (int i = 0; i < list.Count; i++)
								        {
									        var parsedRange = list[i];
									        if (parsedRange.IsMatch(term.Text)) 
									        {
									            var facetValue = Results.Results[term.Field].Values[i];
                                                UpdateValue(facetValue, value, doc, currentIndexSearcher.IndexReader);
									        }
								        }
							        }
						            break;
						        default:
						            throw new ArgumentOutOfRangeException();
						    }
						});
				}
				UpdateFacetResults(facetsByName);

                foreach (var result in Results.Results)
			    {
                    CompleteSingleFacetCalc(result.Value.Values, Facets[result.Key]);
			    }
			}

		    private static void CompleteSingleFacetCalc(IEnumerable<FacetValue> valueCollection, Facet facet)
		    {
		        foreach (var facetValue in valueCollection)
		        {
		            switch (facet.Aggregation)
		            {
		                case FacetAggregation.Average:
		                    if (facetValue.Hits != 0)
		                        facetValue.Value = facetValue.Value/facetValue.Hits;
		                    else
		                        facetValue.Value = double.NaN;
		                    break;
		                    //nothing to do here
		                case FacetAggregation.Count:
		                case FacetAggregation.Max:
		                case FacetAggregation.Min:
		                case FacetAggregation.Sum:
		                    break;
		                default:
		                    throw new ArgumentOutOfRangeException();
		            }
		        }
		    }

		    private static void UpdateValue(FacetValue facetValue, Facet value, int docId, IndexReader indexReader)
		    {
		        facetValue.Hits++;
		        if (value.Aggregation == FacetAggregation.Count)
		        {
		            facetValue.Value = facetValue.Hits;
		            return;
		        }

		        var doc = indexReader.Document(docId);
		        if (doc == null)
		            return;

		        var fieldables = doc.GetFieldables(value.AggregationField);
		        if (fieldables.Length == 0)
                    throw new InvalidOperationException("Cannot compute " + value.Aggregation + " on " +
                                                      value.AggregationField + " because the field is not stored");
                 
		        facetValue.Hits += fieldables.Length - 1;

		        foreach (var field in fieldables)
		        {
		            double numericValue;
		            if (double.TryParse(field.StringValue, NumberStyles.Number, CultureInfo.InvariantCulture, out numericValue) == false)
		                throw new InvalidOperationException("Cannot compute " + value.Aggregation + " on " +
		                                                    value.AggregationField + " because the field value could not be converted to a number: " +
		                                                    field.StringValue);

		            switch (value.Aggregation)
		            {
		                case FacetAggregation.Max:
				            if (facetValue.Value == null)
					            facetValue.Value = 0;
		                    facetValue.Value = Math.Max(facetValue.Value.Value, numericValue);
		                    break;
		                case FacetAggregation.Min:
				            if (facetValue.Value == null)
					            facetValue.Value = double.MaxValue;
                            facetValue.Value = Math.Min(facetValue.Value.Value, numericValue);
		                    break;
                        case FacetAggregation.Average:
		                case FacetAggregation.Sum:
				            if (facetValue.Value == null)
					            facetValue.Value = 0;
		                    facetValue.Value += numericValue;
		                    break;
		                default:
		                    throw new ArgumentOutOfRangeException();
		            }
		        }
		    }


		    private void UpdateFacetResults(Dictionary<string, Dictionary<string, FacetValue>> facetsByName)
			{
				foreach (var facet in Facets.Values)
				{
				    if (facet.Mode == FacetMode.Ranges)
				        continue;

					var values = new List<FacetValue>();
					List<string> allTerms;

					int maxResults = Math.Min(PageSize ?? facet.MaxResults ?? Database.Configuration.MaxPageSize, Database.Configuration.MaxPageSize);
					var groups = facetsByName.GetOrDefault(facet.Name);

					if (groups == null)
						continue;

					switch (facet.TermSortMode)
					{
						case FacetTermSortMode.ValueAsc:
							allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value.Value).Select(x => x.Key));
							break;
						case FacetTermSortMode.ValueDesc:
							allTerms = new List<string>(groups.OrderByDescending(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
							break;
						case FacetTermSortMode.HitsAsc:
							allTerms = new List<string>(groups.OrderBy(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
							break;
						case FacetTermSortMode.HitsDesc:
							allTerms = new List<string>(groups.OrderByDescending(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
							break;
						default:
							throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
					}

					foreach (var term in allTerms.Skip(Start).TakeWhile(term => values.Count < maxResults))
					{
					    var facetValue = groups.GetOrDefault(term);
					    values.Add(new FacetValue
						{
							Hits = facetValue == null ? 0 : facetValue.Hits,
							Value = facetValue == null ? 0 : facetValue.Value,
							Range = term
						});
					}

				    var previousHits = allTerms.Take(Start).Sum(allTerm =>
					{
					    var facetValue = groups.GetOrDefault(allTerm);
					    return facetValue == null ? 0 : facetValue.Hits;
					});
					Results.Results[facet.Name] = new FacetResult
					{
						Values = values,
						RemainingTermsCount = allTerms.Count - (Start + values.Count),
						RemainingHits = groups.Values.Sum(x=>x.Hits) - (previousHits + values.Sum(x => x.Hits)),
					};

					if (facet.IncludeRemainingTerms)
						Results.Results[facet.Name].RemainingTerms = allTerms.Skip(Start + values.Count).ToList();
				}
			}
		}
	}


}
