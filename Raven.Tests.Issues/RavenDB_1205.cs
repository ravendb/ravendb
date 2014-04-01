// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1205.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1205 : RavenTest
	{
		class Item
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Theory]
		[InlineData("embeded")]
		[InlineData("remote")]
		public void CanGetScoreExplanationFromLuceneQuery(string documentStoreType)
		{
			using (var store = documentStoreType == "embeded" ? (IDocumentStore)NewDocumentStore() : NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Name_1"
					});

					session.Store(new Item
					{
						Name = "Name_2"
					});

					session.SaveChanges();

                    var queryResult = session.Advanced.DocumentQuery<Item>().ExplainScores().WaitForNonStaleResults().QueryResult;

                    Assert.False(queryResult.IsStale);
					Assert.Equal(2, queryResult.ScoreExplanations.Count);
					Assert.False(string.IsNullOrEmpty(queryResult.ScoreExplanations["items/1"]));
					Assert.False(string.IsNullOrEmpty(queryResult.ScoreExplanations["items/2"]));
				}
			}
		}

		[Theory]
		[InlineData("embeded")]
		[InlineData("remote")]
		public void CanGetScoreExplanationByUsingDatabaseCommandsQuery(string documentStoreType)
		{
			using (var store = documentStoreType == "embeded" ? (IDocumentStore) NewDocumentStore() : NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Name_1"
					});

					session.Store(new Item
					{
						Name = "Name_2"
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);

				var queryResult = store.DatabaseCommands.Query("Raven/DocumentsByEntityName", new IndexQuery()
				{
					ExplainScores = true,
				}, null);

				Assert.Equal(2, queryResult.ScoreExplanations.Count);
				Assert.False(string.IsNullOrEmpty(queryResult.ScoreExplanations["items/1"]));
				Assert.False(string.IsNullOrEmpty(queryResult.ScoreExplanations["items/2"]));
			}
		}
	}
}