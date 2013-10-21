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
				var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

				defaultFacets[key] = facet;
				if (facet.Aggregation != FacetAggregation.Count && facet.Aggregation != FacetAggregation.None)
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
						results.Results[key] = new FacetResult();
						break;
					case FacetMode.Ranges:
						rangeFacets[key] = facet.Ranges.Select(range => ParseRange(facet.Name, range)).ToList();
						results.Results[key] = new FacetResult
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
			private readonly Dictionary<FacetValue, FacetValueState> matches = new Dictionary<FacetValue, FacetValueState>();
			private readonly IndexDefinition indexDefinition;

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
				indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(this.Index);
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
				ValidateFacets();

				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				var allCollector = new GatherAllCollector();
				var facetsByName = new Dictionary<string, Dictionary<string, FacetValue>>();


				using (var currentState = Database.IndexStorage.GetCurrentStateHolder(Index))
				{
					var currentIndexSearcher = currentState.IndexSearcher;

					var baseQuery = Database.IndexStorage.GetLuceneQuery(Index, IndexQuery, Database.IndexQueryTriggers);
					currentIndexSearcher.Search(baseQuery, allCollector);
					var fields = Facets.Values.Select(x => x.Name)
							.Concat(Ranges.Select(x => x.Key));
					var fieldsToRead = new HashSet<string>(fields);
					IndexedTerms.ReadEntriesForFields(currentState,
						fieldsToRead,
						allCollector.Documents,
						(term, doc) =>
						{
							var facets = Facets.Values.Where(facet => facet.Name == term.Field);
							foreach (var facet in facets)
							{
								switch (facet.Mode)
								{
									case FacetMode.Default:
										var facetValues = facetsByName.GetOrAdd(facet.DisplayName);
										FacetValue existing;
										if (facetValues.TryGetValue(term.Text, out existing) == false)
										{
											existing = new FacetValue
											{
												Range = GetRangeName(term)
											};
											facetValues[term.Text] = existing;
										}
										ApplyFacetValueHit(existing, facet, doc, null);
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
													ApplyFacetValueHit(facetValue, facet, doc, parsedRange);
												}
											}
										}
										break;
									default:
										throw new ArgumentOutOfRangeException();
								}
							}

						});
					UpdateFacetResults(facetsByName);

					CompleteFacetCalculationsStage1(currentState);
					CompleteFacetCalculationsStage2();
				}
			}

			private void ValidateFacets()
			{
				foreach (var facet in Facets.Where(facet => IsAggregationNumerical(facet.Value.Aggregation) && IsAggregationTypeNumerical(facet.Value.AggregationType) && GetSortOptionsForFacet(facet.Value.AggregationField) == SortOptions.None))
				{
					throw new InvalidOperationException(string.Format("Index '{0}' does not have sorting enabled for a numerical field '{1}'.", this.Index, facet.Value.AggregationField));
				}
			}

			private static bool IsAggregationNumerical(FacetAggregation aggregation)
			{
				switch (aggregation)
				{
					case FacetAggregation.Average:
					case FacetAggregation.Count:
					case FacetAggregation.Max:
					case FacetAggregation.Min:
					case FacetAggregation.Sum:
						return true;
					default:
						return false;
				}
			}

			private static bool IsAggregationTypeNumerical(string aggregationType)
			{
				var type = Type.GetType(aggregationType, false, true);
				if (type == null)
					return false;

				var numericalTypes = new List<Type>
				                     {
					                     typeof(decimal),
					                     typeof(int),
					                     typeof(long),
					                     typeof(short),
					                     typeof(float),
					                     typeof(double)
				                     };

				return numericalTypes.Any(numericalType => numericalType == type);
			}

			private string GetRangeName(Term term)
			{
				var sortOptions = GetSortOptionsForFacet(term.Field);
				switch (sortOptions)
				{
					case SortOptions.String:
					case SortOptions.None:
					case SortOptions.Custom:
					case SortOptions.StringVal:
						return term.Text;
					case SortOptions.Int:
						return NumericUtils.PrefixCodedToInt(term.Text).ToString(CultureInfo.InvariantCulture);
					case SortOptions.Long:
						return NumericUtils.PrefixCodedToLong(term.Text).ToString(CultureInfo.InvariantCulture);
					case SortOptions.Double:
						return NumericUtils.PrefixCodedToDouble(term.Text).ToString(CultureInfo.InvariantCulture);
					case SortOptions.Float:
						return NumericUtils.PrefixCodedToFloat(term.Text).ToString(CultureInfo.InvariantCulture);
					case SortOptions.Byte:
					case SortOptions.Short:
					default:
						throw new ArgumentException("Can't get range name from sort option" + sortOptions);
				}
			}

			private void CompleteFacetCalculationsStage2()
			{
				foreach (var facetResult in Results.Results)
				{
					var key = facetResult.Key;
					foreach (var facet in Facets.Values.Where(f => f.DisplayName == key))
					{
						if (facet.Aggregation.HasFlag(FacetAggregation.Count))
						{
							foreach (var facetValue in facetResult.Value.Values)
							{
								facetValue.Count = facetValue.Hits;
							}
						}

						if (facet.Aggregation.HasFlag(FacetAggregation.Average))
						{
							foreach (var facetValue in facetResult.Value.Values)
							{
								if (facetValue.Hits == 0)
									facetValue.Average = double.NaN;
								else
									facetValue.Average = facetValue.Average / facetValue.Hits;
							}
						}
					}
				}
			}

			private void CompleteFacetCalculationsStage1(IndexSearcherHolder.IndexSearcherHoldingState state)
			{
				var fieldsToRead = new HashSet<string>(Facets
						.Where(x => x.Value.Aggregation != FacetAggregation.None && x.Value.Aggregation != FacetAggregation.Count)
						.Select(x => x.Value.AggregationField)
						.Where(x => x != null));

				if (fieldsToRead.Count == 0)
					return;

				var allDocs = new HashSet<int>(matches.Values.SelectMany(x => x.Docs));

				IndexedTerms.ReadEntriesForFields(state, fieldsToRead, allDocs, GetValueFromIndex, (term, currentVal, docId) =>
				{
					foreach (var match in matches)
					{
						if (match.Value.Docs.Contains(docId) == false)
							continue;
						var facet = match.Value.Facet;
						if (term.Field != facet.AggregationField)
							continue;
						switch (facet.Mode)
						{
							case FacetMode.Default:
								ApplyAggregation(facet, match.Key, currentVal);
								break;
							case FacetMode.Ranges:
								if (!match.Value.Range.IsMatch(term.Text))
									continue;
								ApplyAggregation(facet, match.Key, currentVal);
								break;
							default:
								throw new ArgumentOutOfRangeException();
						}
					}
				});
			}

			private void ApplyAggregation(Facet facet, FacetValue value, double currentVal)
			{
				if (facet.Aggregation.HasFlag(FacetAggregation.Max))
				{
					value.Max = Math.Max(value.Max ?? Double.MinValue, currentVal);
				}

				if (facet.Aggregation.HasFlag(FacetAggregation.Min))
				{
					value.Min = Math.Min(value.Min ?? Double.MaxValue, currentVal);
				}

				if (facet.Aggregation.HasFlag(FacetAggregation.Sum))
				{
					value.Sum = currentVal + (value.Sum ?? 0d);
				}

				if (facet.Aggregation.HasFlag(FacetAggregation.Average))
				{
					value.Average = currentVal + (value.Average ?? 0d);
				}
			}

			private double GetValueFromIndex(Term term)
			{
				switch (GetSortOptionsForFacet(term.Field))
				{
					case SortOptions.String:
					case SortOptions.StringVal:
					case SortOptions.Byte:
					case SortOptions.Short:
					case SortOptions.Custom:
					case SortOptions.None:
						throw new InvalidOperationException(string.Format("Cannot perform numeric aggregation on index field '{0}'. You must set the Sort mode of the field to Int, Float, Long or Double.", TryTrimRangeSuffix(term.Field)));
					case SortOptions.Int:
						return NumericUtils.PrefixCodedToInt(term.Text);
					case SortOptions.Float:
						return NumericUtils.PrefixCodedToFloat(term.Text);
					case SortOptions.Long:
						return NumericUtils.PrefixCodedToLong(term.Text);
					case SortOptions.Double:
						return NumericUtils.PrefixCodedToDouble(term.Text);
					default:
						throw new ArgumentOutOfRangeException();
				}
			}

			private readonly Dictionary<string, SortOptions> cache = new Dictionary<string, SortOptions>();
			private SortOptions GetSortOptionsForFacet(string field)
			{
				SortOptions value;
				if (indexDefinition.SortOptions.TryGetValue(field, out value) == false)
				{
					if (field.EndsWith("_Range"))
					{
						var fieldWithNoRange = field.Substring(0, field.Length - "_Range".Length);
						if (indexDefinition.SortOptions.TryGetValue(fieldWithNoRange, out value) == false)
							value = SortOptions.None;
					}
					else
					{
						value = SortOptions.None;
					}
				}
				cache[field] = value;
				return value;
			}

			private string TryTrimRangeSuffix(string fieldName)
			{
				return fieldName.EndsWith("_Range") ? fieldName.Substring(0, fieldName.Length - "_Range".Length) : fieldName;
			}

			private void ApplyFacetValueHit(FacetValue facetValue, Facet value, int docId, ParsedRange parsedRange)
			{
				facetValue.Hits++;
				if (value.Aggregation == FacetAggregation.Count || value.Aggregation == FacetAggregation.None)
				{
					return;
				}
				FacetValueState set;
				if (matches.TryGetValue(facetValue, out set) == false)
				{
					matches[facetValue] = set = new FacetValueState
					{
						Docs = new HashSet<int>(),
						Facet = value,
						Range = parsedRange
					};
				}
				set.Docs.Add(docId);
			}

			private class FacetValueState
			{
				public HashSet<int> Docs;
				public Facet Facet;
				public ParsedRange Range;
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
					var groups = facetsByName.GetOrDefault(facet.DisplayName);

					if (groups == null)
						continue;

					switch (facet.TermSortMode)
					{
						case FacetTermSortMode.ValueAsc:
							allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
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
						values.Add(facetValue ?? new FacetValue { Range = term });
					}

					var previousHits = allTerms.Take(Start).Sum(allTerm =>
					{
						var facetValue = groups.GetOrDefault(allTerm);
						return facetValue == null ? 0 : facetValue.Hits;
					});

					var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

					Results.Results[key] = new FacetResult
					{
						Values = values,
						RemainingTermsCount = allTerms.Count - (Start + values.Count),
						RemainingHits = groups.Values.Sum(x => x.Hits) - (previousHits + values.Sum(x => x.Hits)),
					};

					if (facet.IncludeRemainingTerms)
						Results.Results[key].RemainingTerms = allTerms.Skip(Start + values.Count).ToList();
				}
			}
		}
	}
}
