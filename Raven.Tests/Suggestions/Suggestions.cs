//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.Suggestions
{
	public class Suggestions : RavenTest, IDisposable
	{
		private readonly IDocumentStore documentStore;

		public Suggestions()
		{
			documentStore = NewDocumentStore();

			documentStore.DatabaseCommands.PutIndex("Test", new IndexDefinition
			                                                	{
			                                                		Map = "from doc in docs select new { doc.Name }",
			                                                	});
			using (var s = documentStore.OpenSession())
			{
				s.Store(new User { Name = "Ayende" });
				s.Store(new User { Name = "Oren" });
				s.SaveChanges();

				s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
			}
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			base.Dispose();
		}

		[Fact]
		public void ExactMatch()
		{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("Test",
				                                                                new SuggestionQuery
				                                                                	{
				                                                                		Field = "Name",
				                                                                		Term = "Oren",
				                                                                		MaxSuggestions = 10,
				                                                                	});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
		}

		[Fact]
		public void UsingLinq()
		{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = s.Query<User>("test")
					.Where(x => x.Name == "Oren")
					.Suggest();

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
			}

		[Fact]
		public void UsingLinq_with_typo_with_options_multiple_fields()
		{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = s.Query<User>("test")
					.Where(x => x.Name == "Orin")
					.Where(x => x.Email == "whatever")
					.Suggest(new SuggestionQuery{Field = "Name", Term = "Orin"});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
		}

		[Fact]
		public void UsingLinq_with_typo_multiple_fields_in_reverse_order()
			                                        	{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = s.Query<User>("test")
					.Where(x => x.Email == "whatever")
					.Where(x => x.Name == "Orin")
					.Suggest(new SuggestionQuery { Field = "Name", Term = "Orin" });

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
			}

		[Fact]
		public void UsingLinq_WithOptions()
		{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = s.Query<User>("test")
					.Where(x => x.Name == "Orin")
					.Suggest(new SuggestionQuery {Accuracy = 0.4f});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
		}

		[Fact]
		public void WithTypo()
		{
			using (var s = documentStore.OpenSession())
			{
				var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("Test",
				                                                                new SuggestionQuery
				                                                                	{
				                                                                		Field = "Name",
				                                                                		Term = "Oern",
				                                                                		MaxSuggestions = 10,
				                                                                		Accuracy = 0.2f,
				                                                                		Distance = StringDistanceTypes.Levenshtein
				                                                                	});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
		}

		[Fact]
		public void ExactMatchDynamic()
		{
			using (var s = documentStore.OpenSession())
			{
				s.Store(new User { Name = "Ayende" });
				s.Store(new User { Name = "Oren" });
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var query = s.Query<User>()
					.Where(user => user.Name == "Oren")
					.Customize(x => x.WaitForNonStaleResults());

				GC.KeepAlive(query.FirstOrDefault());
				var suggestionQueryResult = query.Suggest();

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}
		}
	}
}
