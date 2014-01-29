//-----------------------------------------------------------------------
// <copyright file="DynamicQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Queries
{
	public static class DynamicQueryExtensions
	{
		public static QueryResultWithIncludes ExecuteDynamicQuery(this DocumentDatabase self, string entityName, IndexQuery indexQuery, CancellationToken token)
		{
			var dynamicQueryRunner = (DynamicQueryRunner)self.ExtensionsState.GetOrAdd(typeof(DynamicQueryExtensions).AssemblyQualifiedName, o => new DynamicQueryRunner(self));
			return dynamicQueryRunner.ExecuteDynamicQuery(entityName, indexQuery, token);
		}

		public static string FindDynamicIndexName(this DocumentDatabase self, string entityName, IndexQuery query)
		{
            var result = new DynamicQueryOptimizer(self).SelectAppropriateIndex(entityName, query.Clone());
		    if (result.MatchType == DynamicQueryMatchType.Complete)
		        return result.IndexName;
		    return null;
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
