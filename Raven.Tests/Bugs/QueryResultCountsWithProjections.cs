//-----------------------------------------------------------------------
// <copyright file="QueryResultCountsWithProjections.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Xunit;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Client.Linq;
using System.Threading;

namespace Raven.Tests.Bugs
{
	public class QueryResultCountsWithProjections : IDisposable
	{
		EmbeddableDocumentStore store = null;

		public QueryResultCountsWithProjections()
		{
			store = new EmbeddableDocumentStore()
			{
				RunInMemory = true
			};
			store.Initialize();
			PopulateDatastore();
		}

		public void Dispose()
		{
			store.Dispose();
		}

		[Fact]
		public void WhenNoProjectionIsIssuedDuplicateDocumentsAreSkipped()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Blog>("SelectManyIndexWithNoTransformer").ToArray();
				Assert.Equal(1, results.Length);
			}
		}

		[Fact]
		public void WhenProjectionIsIssuedAgainstEntityDuplicateDocumentsAreSkipped()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Blog>("SelectManyIndexWithNoTransformer")
					.Select(x=> new { x.Title, x.Tags })
					.ToArray();
				Assert.Equal(1, results.Length);
			}
		}

		[Fact]
		public void WhenDynamicQueryInvokedAgainstTagsDuplicateDocumentsAreSkipped()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Blog>()
					 .Customize(x=>x.WaitForNonStaleResults())
					 .Where(x => x.Tags.Any(y => y.Name.StartsWith("t")))
					 .Select(x=> new {
						 x.Title,
						 x.Tags
					 })
					 .ToArray();

				Assert.Equal(1, results.Length);
			}
		}

		[Fact]
		public void WhenProjectionIsIssuedAgainstEntityWithTransformResultsDuplicateDocumentsAreSkipped()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Blog>("SelectManyIndexWithTransformer")
					.As<BlogProjection>()
					.ToArray();
				Assert.Equal(1, results.Length);
			}
		}

		[Fact]
		public void WhenProjectionIsIssuedAgainstIndexAndNotEntityDuplicateDocumentsAreNotSkipped()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<BlogProjection>("SelectManyIndexWithNoTransformer")
					.Select(x=> new 
					{
						 x.Title,
						 x.Tag
					})
					.ToArray();
				Assert.Equal(2, results.Length);
			}
		}

		private void PopulateDatastore()
		{
			store.DatabaseCommands.PutIndex("SelectManyIndexWithNoTransformer",
				new IndexDefinitionBuilder<Blog,BlogProjection>()
				{
					Map = docs => from doc in docs
								  from tag in doc.Tags
								  select new
								  {
									  doc.Title,
									  tag.Name
								  },
					Stores = {{x=>x.Tag, FieldStorage.Yes}}
				}.ToIndexDefinition(store.Conventions));

			 store.DatabaseCommands.PutIndex("SelectManyIndexWithTransformer",
			   new IndexDefinitionBuilder<Blog, Blog>()
			   {
				   Map = docs => from doc in docs
								 from tag in doc.Tags
								 select new
								 {
									 doc.Title,
									 tag.Name
								 },
				   TransformResults = (database, results) => from result in results
												 select new
												 {
													 result.Title,
													 result.Tags
												 }
			   }.ToIndexDefinition(store.Conventions));

			 using (var session = store.OpenSession())
			 {
				 session.Store(new Blog()
				 {                    
					 Title = "1",
					 Tags = new BlogTag[]{
						  new BlogTag() { Name = "tagOne" },
						  new BlogTag() { Name = "tagTwo" }
					  }
				 });
				 session.SaveChanges();
			 }

			 while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
			 {
				 Thread.Sleep(10);
			 }
		}

		public class BlogProjection
		{
			public string Title { get; set; }
			public string Tag { get; set; }
		}


		public class Blog
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public BlogTag[] Tags { get; set; }
		}
		
		public class BlogTag 
		{ 
			public string Name { get; set; }
		}


	}
}
