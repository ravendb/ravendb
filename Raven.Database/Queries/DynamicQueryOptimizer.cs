using System.Runtime.CompilerServices;
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

			var fieldsQueriedUpon = SimpleQueryParser.GetFieldsForDynamicQuery(indexQuery.Query).Select(x => x.Item2);

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
						if (abstractViewGenerator.CountOfSelectMany > 1) 
							return false;

						if (abstractViewGenerator.ForEntityNames.Count != 1 || // we only allow indexes with a single entity name
							abstractViewGenerator.ForEntityNames[0] != entityName) // for the specified entity name
							return false;

						return fieldsQueriedUpon.All(abstractViewGenerator.ContainsFieldOnMap);
					})
					.Where(indexName =>
					{
						var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (indexDefinition == null)
							return false;
						
						if (indexQuery.SortedFields != null)
						{
							var sortInfo = DynamicQueryMapping.GetSortInfo(s => { });

							foreach (var sortedField in indexQuery.SortedFields) // with matching sort options
							{
								SortOptions value;

								var dynamicSortInfo = sortInfo.FirstOrDefault(x=>x.Field == sortedField.Field);
								if((indexDefinition.SortOptions.TryGetValue(sortedField.Field, out value) == false || value == SortOptions.None) && 
									dynamicSortInfo == null)
									continue; // no special sorting specified, this is okay

								if (dynamicSortInfo == null)
								{
									if (value == SortOptions.None || value == SortOptions.String)
										continue;// this is the default, so None == String for most cases
									return false;
								}

								switch (value)
								{
									case SortOptions.String:
									case SortOptions.None:
										switch (dynamicSortInfo.FieldType)
										{
											case SortOptions.None:
											case SortOptions.String:
												continue;
										}
										break;
								}

								return false; // different sort order, there is a problem here
							}
						}

						if(indexDefinition.Analyzers != null)
						{
							// none of the fields have custom analyzers
							if (fieldsQueriedUpon.Any(indexDefinition.Analyzers.ContainsKey)) 
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