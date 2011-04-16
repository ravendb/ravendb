//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Database.Indexing;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;

namespace Raven.Tests.Suggestions
{
	public class Suggestions : LocalClientTest, IDisposable
	{
		#region IDisposable Members

		public void Dispose()
		{

		}

		#endregion

		protected DocumentStore DocumentStore { get; set; }

		[Fact]
		public void ExactMatch()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }",
				});
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { Name = "Oren" });
					s.SaveChanges();

					s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var s = store.OpenSession())
				{
					var suggestionQueryResult = s.Advanced.DatabaseCommands.Suggest("Test",
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
		}

		[Fact]
		public void UsingLinq()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }",
				});
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { Name = "Oren" });
					s.SaveChanges();

					s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var s = store.OpenSession())
				{
					var suggestionQueryResult = s.Query<User>("test")
						.Where(x => x.Name == "Oren")
						.Suggest();

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}

		[Fact]
		public void UsingLinq_WithOptions()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }",
				});
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { Name = "Oren" });
					s.SaveChanges();

					s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var s = store.OpenSession())
				{
					var suggestionQueryResult = s.Query<User>("test")
						.Where(x => x.Name == "Orin")
						.Suggest(new SuggestionQuery { Accuracy = 0.4f });

					Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
					Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
				}
			}
		}


		[Fact]
		public void WithTypo()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }",
				});
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { Name = "Oren" });
					s.SaveChanges();

					s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				using (var s = store.OpenSession())
				{
					var suggestionQueryResult = s.Advanced.DatabaseCommands.Suggest("Test",
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
		}
	}
}
