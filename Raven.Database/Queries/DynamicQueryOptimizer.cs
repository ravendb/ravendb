using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using System.Linq;
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
			// aggregation. Therefor we can't reuse one aggregate index for another query.
			// We decline to suggest an index here and choose to use the default index created for this
			// sort of query, which is what we would have to choose anyway.
			if(indexQuery.AggregationOperation != AggregationOperation.None)
				return null;
			
			return database.IndexDefinitionStorage.IndexNames
					.Where(indexName =>
					{
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there is a matching view generator
							return false;

						if (abstractViewGenerator.ReduceDefinition != null) // we can't choose a map/reduce index
							return false;

						if (abstractViewGenerator.TransformResultsDefinition != null)// we can't choose an index with transform results
							return false;

						if (abstractViewGenerator.ViewText.Contains("where")) // without a where clause
							return false;

						if (abstractViewGenerator.ViewText.Contains("IEnumerable")) // we can't choose an index that flattens the document
							return false;

						if (abstractViewGenerator.ForEntityName != entityName) // for the specified entity name
							return false;
						var items = SimpleQueryParser.GetFieldsForDynamicQuery(indexQuery.Query).Select(x => x.Item2);

						return items.All(abstractViewGenerator.ContainsFieldOnMap);
					})
					.Where(indexName =>
					{
						var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (indexDefinition == null)
							return false;
						
						if (indexQuery.SortedFields != null)
						{
							foreach (var sortedField in indexQuery.SortedFields) // with matching sort options
							{
								SortOptions value;
								if (indexDefinition.SortOptions.TryGetValue(sortedField.Field, out value) == false)
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
						if (abstractViewGenerator == null) // there is a matching view generator
							return -1;
						return abstractViewGenerator.CountOfFields;
					})
				.FirstOrDefault();

		}
	}
}