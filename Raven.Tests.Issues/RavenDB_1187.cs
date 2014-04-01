// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1187.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Connection;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Generic;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Indexing;

	using Xunit;

	public class RavenDB_1187 : RavenTest
	{
		[Fact]
		public void QueryingForSuggestionsAgainstFieldWithSuggestionsTurnedOnShouldNotThrow()
		{
			Assert.DoesNotThrow(() =>
				   {
					   using (var store = this.NewDocumentStore())
					   {
						   store.DatabaseCommands.PutIndex("Test", new IndexDefinition
						   {
							   Map = "from doc in docs select new { doc.Name, doc.Other }",
							   Suggestions = new Dictionary<string, SuggestionOptions> { { "Name", new SuggestionOptions() } }
						   });

						   store.DatabaseCommands.Suggest("Test", new SuggestionQuery
						   {
							   Field = "Name",
							   Term = "Oren",
							   MaxSuggestions = 10,
						   });
					   }
				   });
		}

		[Fact]
		public void QueryingForSuggestionsAgainstFieldWithSuggestionsTurnedOffShouldThrow()
		{
			var e = Assert.Throws<ErrorResponseException>(() =>
					{
						using (var store = this.NewDocumentStore())
						{
							store.DatabaseCommands.PutIndex("Test", new IndexDefinition
							{
								Map = "from doc in docs select new { doc.Name, doc.Other }",
								Suggestions = new Dictionary<string, SuggestionOptions> { { "Name", new SuggestionOptions() } }
							});

							store.DatabaseCommands.Suggest("Test", new SuggestionQuery
							{
								Field = "Other",
								Term = "Oren",
								MaxSuggestions = 10,
							});
						}
					});

			Assert.Contains("Index 'Test' does not have suggestions configured for field 'Other'.", e.Message);
		}
	}
}