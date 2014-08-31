// -----------------------------------------------------------------------
//  <copyright file="CanChangeCollectionName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Document
{
	public class CanChangeCollectionName : RavenTest
	{
		[Fact]
		public void AndHaveItGoneFromIndexes()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("users", new IndexDefinition
				{
					Map = "from u in docs.Users select new { u.Name} "
				});

				store.DatabaseCommands.Put("test", null, new RavenJObject { { "Name", "Oren" } }, new RavenJObject
				{
					{Constants.RavenEntityName, "Users"}
				});

				WaitForIndexing(store);

				QueryResult queryResult = store.DatabaseCommands.Query("users", new IndexQuery
				{
					FieldsToFetch = new[] {"__document_id"}
				});
				Assert.Equal(1, queryResult.TotalResults);

				store.DatabaseCommands.Put("test", null, new RavenJObject { { "Name", "Oren" } }, new RavenJObject
				{
					{Constants.RavenEntityName, "Customers"}
				});

				WaitForIndexing(store);

				queryResult = store.DatabaseCommands.Query("users", new IndexQuery
				{
					FieldsToFetch = new[] { "__document_id" }
				});
				Assert.Equal(0, queryResult.TotalResults);
			}
		}
	}
}