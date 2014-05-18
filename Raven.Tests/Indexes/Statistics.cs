//-----------------------------------------------------------------------
// <copyright file="Statistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Common;
using Raven.Tests.Storage;
using Xunit;
using System.Linq;

namespace Raven.Tests.Indexes
{
	public class Statistics : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;
	    private int pagesByTitle2 = 0;

		public Statistics()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;

			db.Indexes.PutIndex("pagesByTitle2",
						new IndexDefinition
						{
							Map = @"
					from doc in docs
					where doc.type == ""page""
					select new {  f = 2 / doc.size };
				"
						});
		    pagesByTitle2 = db.IndexDefinitionStorage.GetIndexDefinition("pagesByTitle2").IndexId;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Can_get_stats_for_indexing_without_any_indexing()
		{
			Assert.True(db.Statistics.Indexes.Any(x => x.Id == pagesByTitle2));
			Assert.Equal(0, db.Statistics.Indexes.First((x => x.Id == pagesByTitle2)).IndexingAttempts);
		}

		[Fact]
		public void Can_get_stats_for_indexing()
		{
			db.Documents.Put("1", Etag.Empty,
				   RavenJObject.Parse(
					@"{
				type: 'page', 
				some: 'val', 
				other: 'var', 
				content: 'this is the content', 
				title: 'hello world', 
				size: 1,
				'@metadata': {'@id': 1}
			}"),
				   new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Queries.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				}, CancellationToken.None);
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

			var indexStats = db.Statistics.Indexes.First(x => x.Id == pagesByTitle2);
			Assert.Equal(1, indexStats.IndexingAttempts);
			Assert.Equal(1, indexStats.IndexingSuccesses);
		}

		[Fact]
		public void Can_get_stats_for_indexing_including_errors()
		{
			db.Documents.Put("1", Etag.Empty,
				   RavenJObject.Parse(
					@"{
				type: 'page', 
				some: 'val', 
				other: 'var', 
				content: 'this is the content', 
				title: 'hello world', 
				size: 0,
				'@metadata': {'@id': 1}
			}"),
				   new RavenJObject(), null);
			db.Documents.Put("2", Etag.Empty,
				   RavenJObject.Parse(
					@"{
				type: 'page', 
				some: 'val', 
				other: 'var', 
				content: 'this is the content', 
				title: 'hello world', 
				size: 1,
				'@metadata': {'@id': 1}
			}"),
				   new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Queries.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				}, CancellationToken.None);
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

			var indexStats = db.Statistics.Indexes.First(x => x.Id == pagesByTitle2);
			Assert.Equal(2, indexStats.IndexingAttempts);
			Assert.Equal(1, indexStats.IndexingErrors);
			Assert.Equal(1, indexStats.IndexingSuccesses);
		}

		[Fact]
		public void Can_get_details_about_indexing_errors()
		{
			db.Documents.Put("1", Etag.Empty,
				   RavenJObject.Parse(
					@"{
				type: 'page', 
				some: 'val', 
				other: 'var', 
				content: 'this is the content', 
				title: 'hello world', 
				size: 0,
				'@metadata': {'@id': 1}
			}"),
				   new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Queries.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				}, CancellationToken.None);
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

			Assert.Equal("1", db.Statistics.Errors[0].Document);
			Assert.Contains("Attempted to divide by zero.", db.Statistics.Errors[0].Error);
		}
	}
}
