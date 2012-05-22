using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Org.BouncyCastle.Utilities.Collections;
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

		public string SelectAppropriateIndex(string entityName, IndexQuery indexQuery)
		{
			// There isn't much point for query optimizer of aggregation indexes
			// the main reason is that we must always aggregate on the same items, and using the same 
			// aggregation. Therefore we can't reuse one aggregate index for another query.
			// We decline to suggest an index here and choose to use the default index created for this
			// sort of query, which is what we would have to choose anyway.
			if(indexQuery.AggregationOperation != AggregationOperation.None)
				return null;

			if (string.IsNullOrEmpty(indexQuery.Query) && // we optimize for empty queries to use Raven/DocumentsByEntityName
			    (indexQuery.SortedFields == null || indexQuery.SortedFields.Length == 0) && // and no sorting was requested
				database.IndexDefinitionStorage.Contains("Raven/DocumentsByEntityName")) // and Raven/DocumentsByEntityName exists
			{
				if (string.IsNullOrEmpty(entityName) == false)
					indexQuery.Query = "Tag:" + entityName;
				return "Raven/DocumentsByEntityName";
			}

			var fieldsQueriedUpon = SimpleQueryParser.GetFieldsForDynamicQuery(indexQuery.Query).Select(x => x.Item2).ToArray();
			var normalizedFieldsQueriedUpon =
				fieldsQueriedUpon.Select(DynamicQueryMapping.ReplaceIndavlidCharactersForFields).ToArray();
			var distinctSelectManyFields = new HashSet<string>();
			foreach (var field in fieldsQueriedUpon)
			{
				var parts = field.Split(new[]{','}, StringSplitOptions.RemoveEmptyEntries);
				for (int i = 1; i < parts.Length; i++)
				{
					distinctSelectManyFields.Add(string.Join(",", parts.Take(i)));
				}
			}

			return database.IndexDefinitionStorage.IndexNames
					.Where(indexName =>
					{
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there is no matching view generator
							return false;

						if (abstractViewGenerator.ReduceDefinition != null) // we can't choose a map/reduce index
							return false;

						if (abstractViewGenerator.TransformResultsDefinition != null)// we can't choose an index with transform results
							return false;

						if (abstractViewGenerator.HasWhereClause) // without a where clause
							return false;

						// we can't select an index that has SelectMany in it, because it result in invalid results when
						// you query it for things like Count, see https://github.com/ravendb/ravendb/issues/250
						// for indexes with internal projections, we use the exact match based on the generated index name
						// rather than selecting the optimal one
						// in order to handle that, we count the number of select many that would happen because of the query
						// and match it to the number of select many in the index
						if (abstractViewGenerator.CountOfSelectMany != distinctSelectManyFields.Count)
							return false;

						if(entityName == null)
						{
							if (abstractViewGenerator.ForEntityNames.Count != 0)
								return false;
						}
						else
						{
							if (abstractViewGenerator.ForEntityNames.Count != 1 || // we only allow indexes with a single entity name
								abstractViewGenerator.ForEntityNames.Contains(entityName) == false) // for the specified entity name
								return false;
						}

						if (normalizedFieldsQueriedUpon.All(abstractViewGenerator.ContainsFieldOnMap) == false)
							return false;
					
						var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (indexDefinition == null)
							return false;

						if (indexQuery.SortedFields != null && indexQuery.SortedFields.Length> 0)
						{
							var sortInfo = DynamicQueryMapping.GetSortInfo(s => { });

							foreach (var sortedField in indexQuery.SortedFields) // with matching sort options
							{
								if(sortedField.Field.StartsWith(Constants.RandomFieldName))
									continue;

								// if the field is not in the output, then we can't sort on it. 
								if (abstractViewGenerator.ContainsField(sortedField.Field) == false)
									return false;

								var dynamicSortInfo = sortInfo.FirstOrDefault(x=>x.Field == sortedField.Field);

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
											return false;
									}
								}

								if(value != dynamicSortInfo.FieldType)
									return false; // different sort order, there is a problem here
							}
						}

						if (indexDefinition.Analyzers != null && indexDefinition.Analyzers.Count > 0)
						{
							// none of the fields have custom analyzers
							if (normalizedFieldsQueriedUpon.Any(indexDefinition.Analyzers.ContainsKey)) 
								return false;
						}

						if (indexDefinition.Indexes != null && indexDefinition.Indexes.Count > 0)
						{
							//If any of the fields we want to query on are set to something other than "NotAnalysed", don't use the index
							var anyFieldWithNonDefaultIndexing = normalizedFieldsQueriedUpon.Any(x =>
							{
								if (indexDefinition.Indexes.ContainsKey(x))
								{
									var analysedInfo = indexDefinition.Indexes[x];
									if (analysedInfo != FieldIndexing.NotAnalyzed)
										return true;
								}
								return false;
							});

							if (anyFieldWithNonDefaultIndexing)
								return false;
						}
						return true;
					})
					.OrderByDescending(indexName =>
					{
						// We select the widest index, because we want to encourage bigger indexes
						// Over time, it means that we smaller indexes would wither away and die, while
						// bigger indexes will be selected more often and then upgrade to permanent indexes
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there is a matching view generator
							return -1;
						return abstractViewGenerator.CountOfFields;
					})
				.FirstOrDefault();

		}
	}
}