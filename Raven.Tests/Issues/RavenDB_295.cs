// -----------------------------------------------------------------------
//  <copyright file="RavenDB_295.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_295 : RavenTest
	{
		[Fact]
		public void CanUpdateSuggestions()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new{Name  = "john"});
					session.Store(new { Name = "darsy" });
					session.SaveChanges();
				}
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs select new { doc.Name}"
				                                });

				WaitForIndexing(store);

				var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
				{
					Field = "Name", Term = "orne"
				});
				Assert.Empty(suggestionQueryResult.Suggestions);

				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "oren" });
					session.SaveChanges();
				}
				WaitForIndexing(store);

				suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
				{
					Field = "Name",
					Term = "orne"
				});
				Assert.NotEmpty(suggestionQueryResult.Suggestions);
			}
		}

		[Fact]
		public void CanUpdateSuggestions_AfterRestart()
		{
			using (var store = NewDocumentStore(runInMemory: false, deleteDirectoryOnDispose: false))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new {Name = "john"});
					session.Store(new {Name = "darsy"});
					session.SaveChanges();
				}
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name}"
				});

				WaitForIndexing(store);

				var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
				{
					Field = "Name",
					Term = "jhon"
				});
				Assert.NotEmpty(suggestionQueryResult.Suggestions);
			}

			using (var store = NewDocumentStore(runInMemory: false, deleteDirectoryOnDispose: true, deleteDirectory: false))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "oren" });
					session.SaveChanges();
				}
				WaitForIndexing(store);


				var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
				{
					Field = "Name",
					Term = "jhon"
				});
				Assert.NotEmpty(suggestionQueryResult.Suggestions); 
				
				suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
				{
					Field = "Name",
					Term = "orne"
				});
				Assert.NotEmpty(suggestionQueryResult.Suggestions);
			}
		}
	}
}