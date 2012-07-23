using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Queries
{
	public class DynamicQueryOptimizer
	{
		private readonly DocumentDatabase database;

		public DynamicQueryOptimizer(DocumentDatabase database)
		{
			this.database = database;
		}

		private delegate void ExplainDelegate(string index, Func<string> rejectionReasonGenerator);

		public class Explanation
		{
			public string Index { get; set; }
			public string Reason { get; set; }
		}

		public string SelectAppropriateIndex(
			string entityName,
			IndexQuery indexQuery,
			List<Explanation> explanations = null)
		{
			// There isn't much point for query optimizer of aggregation indexes
			// the main reason is that we must always aggregate on the same items, and using the same 
			// aggregation. Therefore we can't reuse one aggregate index for another query.
			// We decline to suggest an index here and choose to use the default index created for this
			// sort of query, which is what we would have to choose anyway.
			if (indexQuery.AggregationOperation != AggregationOperation.None)
				return null;

			if (string.IsNullOrEmpty(indexQuery.Query) && // we optimize for empty queries to use Raven/DocumentsByEntityName
			    (indexQuery.SortedFields == null || indexQuery.SortedFields.Length == 0) && // and no sorting was requested
				database.IndexDefinitionStorage.Contains("Raven/DocumentsByEntityName")) // and Raven/DocumentsByEntityName exists
			{
				if (string.IsNullOrEmpty(entityName) == false)
					indexQuery.Query = "Tag:" + entityName;
				return "Raven/DocumentsByEntityName";
			}

			var fieldsQueriedUpon = SimpleQueryParser.GetFieldsForDynamicQuery(indexQuery).Select(x => x.Item2).ToArray();
			var normalizedFieldsQueriedUpon =
				fieldsQueriedUpon.Select(DynamicQueryMapping.ReplaceIndavlidCharactersForFields).ToArray();
			var distinctSelectManyFields = new HashSet<string>();
			foreach (var field in fieldsQueriedUpon)
			{
				var parts = field.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
				for (int i = 1; i < parts.Length; i++)
				{
					distinctSelectManyFields.Add(string.Join(",", parts.Take(i)));
				}
			}

			ExplainDelegate explain = (index, rejectionReason) => { };
			if (explanations != null)
			{
				explain = (index, rejectionReason) => explanations.Add(new Explanation
																		{
																			Index = index,
																			Reason = rejectionReason()
																		});
			}


			// there is no reason why we can't use indexes with transform results
			// we merely need to disable the transform results for this particular query
			indexQuery.SkipTransformResults = true;
			var results = database.IndexDefinitionStorage.IndexNames
					.Where(indexName =>
					{
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there is no matching view generator
						{
							explain(indexName, () => "There is no matching view generator. Maybe the index in the process of being deleted?");
							return false;
						}

						if (entityName == null)
						{
							if (abstractViewGenerator.ForEntityNames.Count != 0)
							{
								explain(indexName, () => "Query is not specific for entity name, but the index filter by entity names.");
								return false;
							}
						}
						else
						{
							if (abstractViewGenerator.ForEntityNames.Count > 1) // we only allow indexes with a single entity name
							{
								explain(indexName, () => "Index contains more than a single entity name, may result in a different type being returned.");
								return false;
							}
							if (abstractViewGenerator.ForEntityNames.Count == 0)
							{
								explain(indexName, () => "Query is specific for entity name, but the index searches across all of them, may result in a different type being returned.");
								return false;
							}
							if (abstractViewGenerator.ForEntityNames.Contains(entityName) == false) // for the specified entity name
							{
								explain(indexName, () => string.Format("Index does not apply to entity name: {0}", entityName));
							return false;
							}
						}

						if (abstractViewGenerator.ReduceDefinition != null) // we can't choose a map/reduce index
						{
							explain(indexName, () => "Can't choose a map/reduce index for dynamic queries.");
							return false;
						}

						if (abstractViewGenerator.HasWhereClause) // without a where clause
						{
							explain(indexName, () => "Can't choose an index with a where clause, it might filter things that the query is looking for.");
							return false;
						}

						// we can't select an index that has SelectMany in it, because it result in invalid results when
						// you query it for things like Count, see https://github.com/ravendb/ravendb/issues/250
						// for indexes with internal projections, we use the exact match based on the generated index name
						// rather than selecting the optimal one
						// in order to handle that, we count the number of select many that would happen because of the query
						// and match it to the number of select many in the index
						if (abstractViewGenerator.CountOfSelectMany != distinctSelectManyFields.Count)
						{
							explain(indexName,
									() => "Can't choose an index with a different number of from clauses / SelectMany, will affect queries like Count().");
							return false;
						}

						if (normalizedFieldsQueriedUpon.All(abstractViewGenerator.ContainsFieldOnMap) == false)
						{
							explain(indexName, () =>
						{
													var missingFields =
														normalizedFieldsQueriedUpon.Where(s => abstractViewGenerator.ContainsFieldOnMap(s) == false);
													return "The following fields are missing: " + string.Join(", ", missingFields);
												});
								return false;
						}

						var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (indexDefinition == null)
							return false;

						if (indexQuery.SortedFields != null && indexQuery.SortedFields.Length > 0)
						{
							var sortInfo = DynamicQueryMapping.GetSortInfo(s => { });

							foreach (var sortedField in indexQuery.SortedFields) // with matching sort options
							{
								if (sortedField.Field.StartsWith(Constants.RandomFieldName))
									continue;

								// if the field is not in the output, then we can't sort on it. 
								if (abstractViewGenerator.ContainsField(sortedField.Field) == false)
								{
									explain(indexName,
											() =>
											"Rejected because index does not contains field '" + sortedField.Field + "' which we need to sort on");
									return false;
								}

								var dynamicSortInfo = sortInfo.FirstOrDefault(x => x.Field == sortedField.Field);

								if (dynamicSortInfo == null)// no sort order specified, we don't care, probably
									continue;

								SortOptions value;
								if (indexDefinition.SortOptions.TryGetValue(sortedField.Field, out value) == false)
								{
									switch (dynamicSortInfo.FieldType)// if we can't find the value, we check if we asked for the default sorting
									{
										case SortOptions.String:
										case SortOptions.None:
											continue;
										default:
											explain(indexName,
													() => "The specified sort type is different than the default for field: " + sortedField.Field);
											return false;
									}
								}

								if (value != dynamicSortInfo.FieldType)
								{
									explain(indexName,
											() =>
											"The specified sort type (" + dynamicSortInfo.FieldType + ") is different than the one specified for field '" +
											sortedField.Field + "' (" + value + ")");
									return false; // different sort order, there is a problem here
							}
						}
						}

						if (indexDefinition.Analyzers != null && indexDefinition.Analyzers.Count > 0)
						{
							// none of the fields have custom analyzers
							if (normalizedFieldsQueriedUpon.Any(indexDefinition.Analyzers.ContainsKey)) 
							{
								explain(indexName, () =>
													{
														var fields = normalizedFieldsQueriedUpon.Where(indexDefinition.Analyzers.ContainsKey);
														return "The following field have a custom analyzer: " + string.Join(", ", fields);
													});
								return false;
						}
						}

						if (indexDefinition.Indexes != null && indexDefinition.Indexes.Count > 0)
						{
							//If any of the fields we want to query on are set to something other than the default, don't use the index
							var anyFieldWithNonDefaultIndexing = normalizedFieldsQueriedUpon.Where(x =>
							{
								FieldIndexing analysedInfo;
								if (indexDefinition.Indexes.TryGetValue(x, out analysedInfo))
								{
									if (analysedInfo != FieldIndexing.Default)
										return true;
								}
								return false;
							});

							if (anyFieldWithNonDefaultIndexing.Any())
							{
								explain(indexName, () =>
								{
									var fields = anyFieldWithNonDefaultIndexing.Where(indexDefinition.Analyzers.ContainsKey);
									return "The following field have aren't using default indexing: " + string.Join(", ", fields);
								});
								return false;
						}
						}
						return true;
					})
					.OrderByDescending(indexName =>
					{
						// We select the widest index, because we want to encourage bigger indexes
						// Over time, it means that we smaller indexes would wither away and die, while
						// bigger indexes will be selected more often and then upgrade to permanent indexes
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there isn't a matching view generator
							return -1;
						return abstractViewGenerator.CountOfFields;
					});

			string name = null;

			foreach (var indexName in results)
			{
				if (name == null)
					name = indexName;
				else
				{
					explain(indexName, () => "Wasn't the widest index matching this query.");
				}
			}

			explain(name ?? "Temporary index will be created", () => "Selected as best match");

			return name;
		}
	}
}
