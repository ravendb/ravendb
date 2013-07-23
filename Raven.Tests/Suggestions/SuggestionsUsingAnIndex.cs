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
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.Suggestions
{
	public class SuggestionsUsingAnIndex : RavenTest, IDisposable
	{
		public class DefaultSuggestionIndex : AbstractIndexCreationTask<User>
		{
			public DefaultSuggestionIndex()
			{
				Map = users => from user in users
				               select new {user.Name};

				Suggestion(user => user.Name);
			}
		}

		public class SuggestionIndex : AbstractIndexCreationTask<User>
		{
			public SuggestionIndex()
			{
				Map = users => from user in users
				               select new {user.Name};

				Suggestion(user => user.Name, new SuggestionOptions
				{
					Accuracy = 0.2f,
					Distance = StringDistanceTypes.Levenshtein,
				});
			}
		}

		[Fact]
		public void ExactMatch()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("DefaultSuggestionIndex", new SuggestionQuery
					{
						Field = "Name",
						Term = "Oren",
						MaxSuggestions = 10,
					});

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void UsingLinq()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
					                                   .Where(x => x.Name == "Oren")
					                                   .Suggest();

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void UsingLinq_with_typo_with_options_multiple_fields()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
					                                   .Where(x => x.Name == "Orin")
					                                   .Where(x => x.Email == "whatever")
					                                   .Suggest(new SuggestionQuery {Field = "Name", Term = "Orin"});

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void UsingLinq_with_typo_multiple_fields_in_reverse_order()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
					                                   .Where(x => x.Email == "whatever")
					                                   .Where(x => x.Name == "Orin")
					                                   .Suggest(new SuggestionQuery {Field = "Name", Term = "Orin"});

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void UsingLinq_WithOptions()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new SuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, SuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = session.Query<User, SuggestionIndex>()
					                                   .Where(x => x.Name == "Orin")
					                                   .Suggest(new SuggestionQuery {Accuracy = 0.4f});

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void WithTypo()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new SuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, SuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("SuggestionIndex", new SuggestionQuery
					{
						Field = "Name",
						Term = "Oern", // intentional typo
						MaxSuggestions = 10,
						Accuracy = 0.1f,						  
						Distance = StringDistanceTypes.NGram  
					});

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void ExactMatchDynamic()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var session = documentStore.OpenSession())
				{
					var query = session.Query<User>()
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
}