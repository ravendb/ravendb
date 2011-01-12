using System;
using Raven.Abstractions.Data;
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

		public string SelectAppropriateIndex(DynamicQueryMapping mapping)
		{
			if(mapping.AggregationOperation != AggregationOperation.None)
				return null;

			return 
				database.IndexDefinitionStorage.IndexNames
					.Where(indexName =>
					{
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if(abstractViewGenerator == null) // there is a matching view generator
							return false;

						if (abstractViewGenerator.ForEntityName != mapping.ForEntityName) // for the specified entity name
							return false;

						// TODO: This isn't very well done, we need to have better analysis of the actual query to generate better
						// TODO: field names for the abstractViewGenerator
						foreach (var queryMappingItem in mapping.Items)
						{
							if (abstractViewGenerator.ContainsFieldDirectly(queryMappingItem.To) == false)
								return false;
						}

						return abstractViewGenerator.ViewText.Contains("where") == false; // without a where clause
					})
					.Where(indexName =>
					{
						var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (indexDefinition == null)
							return false;

						foreach (var sortDescriptor in mapping.SortDescriptors) // with matching sort options
						{
							SortOptions value;
							if (indexDefinition.SortOptions.TryGetValue(sortDescriptor.Field, out value) == false)
								return false;

							SortOptions result;
							if (Enum.TryParse(sortDescriptor.FieldType, true, out result) == false)
								return false;

							if (result != value)
								return false;
						}

						return true;
					})
					.OrderByDescending(indexName =>
					{
						// we select the widest index
						var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
						if (abstractViewGenerator == null) // there is a matching view generator
							return -1;
						return abstractViewGenerator.CountOfFields;
					})
				.FirstOrDefault();

		}
	}
}