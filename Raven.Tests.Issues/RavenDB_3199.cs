// -----------------------------------------------------------------------
//  <copyright file="UnknownIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Imports.Newtonsoft.Json;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3199
	{

		[Fact]
		public void ShouldReadLegacySuggestionOptions()
		{
			string legacySuggestions = @"{
  ""IndexId"": 5,
  ""Name"": ""Orders/ByCompany"",
  ""LockMode"": ""Unlock"",
  ""Map"": ""from order in docs.Orders\r\nselect new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }"",
  ""Maps"": [
    ""from order in docs.Orders\r\nselect new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }""
  ],
  ""Reduce"": ""from result in results\r\ngroup result by result.Company into g\r\nselect new\r\n{\r\n\tCompany = g.Key,\r\n\tCount = g.Sum(x=>x.Count),\r\n\tTotal = g.Sum(x=>x.Total)\r\n}"",
  ""IsMapReduce"": true,
  ""IsCompiled"": false,
  ""Stores"": {},
  ""Indexes"": {},
  ""SortOptions"": {},
  ""Analyzers"": {},
  ""Fields"": [
    ""Company"",
    ""Count"",
    ""Total""
  ],
  ""Suggestions"": {
    ""Company"": {
      ""Distance"": ""Levenshtein"",
      ""Accuracy"": 0.3
    }
  },
  ""TermVectors"": {},
  ""SpatialIndexes"": {},
  ""InternalFieldsMapping"": null,
  ""MaxIndexOutputsPerDocument"": null,
  ""Type"": ""MapReduce"",
  ""DisableInMemoryIndexing"": false,
  ""IsTestIndex"": false,
  ""IsSideBySideIndex"": false
}";

			var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(legacySuggestions, Default.Converters);
			Assert.Equal(1, indexDefinition.Suggestions.Count);
			Assert.Equal(1, indexDefinition.SuggestionsOptions.Count);
			Assert.Equal(SuggestionQuery.DefaultDistance, indexDefinition.Suggestions["Company"].Distance);
			Assert.Equal(SuggestionQuery.DefaultAccuracy, indexDefinition.Suggestions["Company"].Accuracy);
			Assert.Equal("Company", indexDefinition.SuggestionsOptions.ToList()[0]);
		}
	}
}