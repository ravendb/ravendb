//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using System.Linq;
using Raven.Abstractions.Indexing;

namespace Raven.Database.Queries
{
	public static class SuggestionQueryExtensions
	{
		public static SuggestionQueryResult ExecuteSuggestionQuery(this DocumentDatabase self, string index, SuggestionQuery suggestionQuery)
		{
			if (index == "dynamic" || index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
			{
				throw new InvalidOperationException("Cannot get suggestions for dynamic indexes, only static indexes with explicitly defined Suggestions are supported");
			}
		    
            var indexDefinition = self.Indexes.GetIndexDefinition(index);
		    if (indexDefinition == null)
		        throw new InvalidOperationException(string.Format("Could not find specified index '{0}'.", index));

		    if (indexDefinition.SuggestionsOptions.Contains(suggestionQuery.Field) == false)
		    {
		        throw new InvalidOperationException(string.Format("Index '{0}' does not have suggestions configured for field '{1}'.", index, suggestionQuery.Field));
		    }

			if (suggestionQuery.Accuracy.HasValue == false)
				suggestionQuery.Accuracy = SuggestionQuery.DefaultAccuracy;

			if (suggestionQuery.Distance.HasValue == false)
				suggestionQuery.Distance = SuggestionQuery.DefaultDistance;


		    return new SuggestionQueryRunner(self).ExecuteSuggestionQuery(index, suggestionQuery);
		}

	}
}
