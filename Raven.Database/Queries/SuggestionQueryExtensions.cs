//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using System.Linq;

namespace Raven.Database.Queries
{
	public static class SuggestionQueryExtensions
	{
		public static SuggestionQueryResult ExecuteSuggestionQuery(this DocumentDatabase self, string index, SuggestionQuery suggestionQuery)
		{
			if (index == "dynamic" || index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase))
			{
				var entitName = index == "dynamic" ? null : index.Remove(0, "dynamic/".Length);
				index = self.FindDynamicIndexName(entitName, new IndexQuery
				{
					Query = suggestionQuery.Field + ":" + QouteIfNeeded(suggestionQuery.Term)
				});
				if(string.IsNullOrEmpty(index))
					throw new InvalidOperationException("Could find no index for the specified query, suggestions will not create a dynamic index, and cannot suggest without an index. Did you forget to query before calling Suggest?");
			}

			return new SuggestionQueryRunner(self).ExecuteSuggestionQuery(index, suggestionQuery);
		}

		private static string QouteIfNeeded(string term)
		{
			if (term.Any(char.IsWhiteSpace))
				return '"' + term + '"';
			return term;
		}
	}
}