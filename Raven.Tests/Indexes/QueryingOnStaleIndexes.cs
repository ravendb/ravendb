//-----------------------------------------------------------------------
// <copyright file="QueryingOnStaleIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class QueryingOnStaleIndexes: RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public QueryingOnStaleIndexes()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void WillGetStaleResultWhenThereArePendingTasks()
		{
			db.Put("a", null, new RavenJObject(), new RavenJObject(), null);

			Assert.True(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
			}).IsStale);
		}

		[Fact]
		public void WillGetNonStaleResultWhenAskingWithCutoffDate()
		{
			db.Put("a", null, new RavenJObject(), new RavenJObject(), null);

			for (int i = 0; i < 500; i++)
			{
				var queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					PageSize = 2,
					Start = 0,
				});
				if (queryResult.IsStale == false)
					break;
				Thread.Sleep(100);
			}

			Assert.False(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
			}).IsStale);

			db.StopBackgroundWorkers();

			db.Put("a", null, new RavenJObject(), new RavenJObject(), null);


			Assert.True(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
			}).IsStale);

			Assert.False(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
				Cutoff = SystemTime.UtcNow.AddHours(-1)
			}).IsStale);
		}
		
	}
}
