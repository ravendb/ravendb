//-----------------------------------------------------------------------
// <copyright file="HierarchicalData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class HierarchicalData : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public HierarchicalData()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanCreateHierarchicalIndexes()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = @"
from post in docs.Posts
from comment in Recurse(post, ((Func<dynamic,dynamic>)(x=>x.Comments)))
select new { comment.Text }"
			});

			db.Put("abc", null, RavenJObject.Parse(@"
{
	'Name': 'Hello Raven',
	'Comments': [
		{ 'Author': 'Ayende', 'Text': 'def',	'Comments': [ { 'Author': 'Rahien', 'Text': 'abc' } ] }
	]
}
"), RavenJObject.Parse("{'Raven-Entity-Name': 'Posts'}"), null);

			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("test", new IndexQuery
				{
					Query = "Text:abc"
				});
			} while (queryResult.IsStale);

			Assert.Equal(1, queryResult.Results.Count);
		}
	}
}