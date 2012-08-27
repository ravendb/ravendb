//-----------------------------------------------------------------------
// <copyright file="DynamicQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Queries
{
	public static class DynamicQueryExtensions
	{
		public static QueryResultWithIncludes ExecuteDynamicQuery(this DocumentDatabase self, string entityName, IndexQuery indexQuery)
		{
			var dynamicQueryRunner = (DynamicQueryRunner)self.ExtensionsState.GetOrAdd(typeof(DynamicQueryExtensions).AssemblyQualifiedName, o => new DynamicQueryRunner(self));
			return dynamicQueryRunner.ExecuteDynamicQuery(entityName, indexQuery);
		}

		public static string FindDynamicIndexName(this DocumentDatabase self, string entityName, IndexQuery query)
		{
			return new DynamicQueryOptimizer(self).SelectAppropriateIndex(entityName, query.Clone());
		}


		public static List<DynamicQueryOptimizer.Explanation> ExplainDynamicIndexSelection(this DocumentDatabase self, string entityName, IndexQuery query)
		{
			var explanations = new List<DynamicQueryOptimizer.Explanation>();
			new DynamicQueryOptimizer(self)
				.SelectAppropriateIndex(entityName, query.Clone(), explanations);
			return explanations;
		}
	}
}
