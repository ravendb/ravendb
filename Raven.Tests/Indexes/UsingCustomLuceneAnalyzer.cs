//-----------------------------------------------------------------------
// <copyright file="UsingCustomLuceneAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class UsingCustomLuceneAnalyzer : RavenTest
	{
		public class Entity
		{
			public string Name;
		}

		public class EntityCount
		{
			public string Name;
			public int Count;
		}

		private string entityName = "som\xC9";  // \xC9, \xC8 are both E characters with differing accents
		private string searchString = "som\xC8";
		private string analyzedName = "some";

		[Fact]
		public void custom_analyzer_folds_ascii()
		{
			var tokens = LuceneAnalyzerUtils.TokensFromAnalysis(new CustomAnalyzer(), entityName);

			Assert.Equal(analyzedName, tokens.Single());
		}
		
		public void with_index_and_some_entities(Action<IDocumentSession> action)
		{
			using (var store = NewDocumentStore())
			{
				var indexDefinition = new IndexDefinitionBuilder<Entity, EntityCount>()
				{
					Map = docs => docs.Select(doc => new { Name = doc.Name, NormalizedName = doc.Name, Count = 1 }),
					Reduce = docs => from doc in docs
									 group doc by new { doc.Name } into g
									 select new { Name = g.Key.Name, NormalizedName = g.Key.Name, Count = g.Sum(c => c.Count) },
					Indexes =
						{
							{e => e.Name, FieldIndexing.NotAnalyzed }
						}
				}.ToIndexDefinition(store.Conventions);

				indexDefinition.Analyzers = new Dictionary<string, string>()
				{
					{"NormalizedName", typeof (CustomAnalyzer).AssemblyQualifiedName}
				};

				store.DatabaseCommands.PutIndex("someIndex", indexDefinition);

				using (var session = store.OpenSession())
				{
					session.Store(new Entity() { Name = entityName });
					session.Store(new Entity() { Name = entityName });
					session.Store(new Entity() { Name = entityName });
					session.Store(new Entity() { Name = entityName });
					session.Store(new Entity() { Name = "someOtherName1" });
					session.Store(new Entity() { Name = "someOtherName2" });
					session.Store(new Entity() { Name = "someOtherName3" });
					session.SaveChanges();
				}
				
				// This wait should update the index with all changes...
				WaitForIndex(store, "someIndex");

				using (var session2 = store.OpenSession())
				{
					action(session2);
				}
			}
		}

		[Fact]
		public void find_matching_document_with_lucene_query()
		{
			with_index_and_some_entities(delegate(IDocumentSession session)
			{
				var result = session.Advanced.LuceneQuery<EntityCount>("someIndex").WaitForNonStaleResults()
					.WhereEquals(new WhereParams
					{
						FieldName = "NormalizedName",
						Value = searchString,
						IsAnalyzed = true,
						AllowWildcards = false
					})
					.ToArray();

				Assert.Equal(1, result.Length);
				Assert.Equal(entityName, result.First().Name);
			});
		}

		[Fact]
		public void map_reduce_used_for_counting()
		{
			with_index_and_some_entities(delegate(IDocumentSession session)
			{
				var result = session.Advanced.LuceneQuery<EntityCount>("someIndex")
					.WaitForNonStaleResults()
					.WhereEquals(new WhereParams
					{
						FieldName = "NormalizedName",
						Value = searchString,
						IsAnalyzed = true,
						AllowWildcards = false
					})
					.ToArray();

				Assert.Equal(4, result.First().Count);
			});
		}

		protected void WaitForIndex(IDocumentStore store, string indexName)
		{
			using (var session = store.OpenSession())
			{
				session.Advanced.LuceneQuery<object>(indexName)
					.Where("NOT \"*\"")
					.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
					.ToArray();
			}
		}
	}
}
