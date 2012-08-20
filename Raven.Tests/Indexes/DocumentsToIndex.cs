//-----------------------------------------------------------------------
// <copyright file="DocumentsToIndex.cs" company="Hibernating Rhinos LTD">
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
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class DocumentsToIndex : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public DocumentsToIndex()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
			db.SpinBackgroundWorkers();
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Can_Read_values_from_index()
		{
			db.PutIndex("pagesByTitle2",
					   new IndexDefinition
					   {
						   Map = @"
					from doc in docs
					where doc.type == ""page""
					select new { doc.some };
				"
					   });
			db.Put("1", Guid.Empty,
			       RavenJObject.Parse(
			       	@"{
				type: 'page', 
				some: 'val', 
				other: 'var', 
				content: 'this is the content', 
				title: 'hello world', 
				size: 5,
				'@metadata': {'@id': 1}
			}"),
				   new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Query = "some:val",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
		}

		[Fact]
		public void Can_update_values_in_index_with_where_clause()
		{
			db.PutIndex("pagesByTitle2",
					   new IndexDefinition
					   {
						   Map = @"
					from doc in docs
					where doc.type == ""page""
					select new { doc.name };
				"
					   });
			 db.Put("1", null,
				   RavenJObject.Parse(
					@"{ type: 'page', name: 'ayende' }"),
				   new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);

			db.Put("1", null,
				   RavenJObject.Parse(
					@"{ type: 'bar', name: 'ayende' }"),
				   new RavenJObject(), null);

			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(0, docs.Results.Count);
		}

		[Fact]
		public void Can_Read_Values_Using_Deep_Nesting()
		{
			db.PutIndex(@"DocsByProject",
						new IndexDefinition
						{
							Map = @"
from doc in docs
from prj in doc.projects
select new{project_name = prj.name}
"
						});

			var document =
				RavenJObject.Parse(
					"{'name':'ayende','email':'ayende@ayende.com','projects':[{'name':'raven'}], '@metadata': { '@id': 1}}");
			db.Put("1", Guid.Empty, document, new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("DocsByProject", new IndexQuery
				{
					Query = "project_name:raven",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
			var jProperty = docs.Results[0]["name"];
			Assert.Equal("ayende", jProperty.Value<string>());
		}

		[Fact]
		public void Can_Read_Values_Using_MultipleValues_From_Deep_Nesting()
		{
			db.PutIndex(@"DocsByProject",
						new IndexDefinition
						{
							Map = @"
from doc in docs
from prj in doc.projects
select new{project_name = prj.name, project_num = prj.num}
"
						});
			var document =
				RavenJObject.Parse(
					"{'name':'ayende','email':'ayende@ayende.com','projects':[{'name':'raven', 'num': 5}, {'name':'crow', 'num': 6}], '@metadata': { '@id': 1}}");
			db.Put("1", Guid.Empty, document, new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("DocsByProject", new IndexQuery
				{
					Query = "+project_name:raven +project_num:6",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(0, docs.Results.Count);
		}

		[Fact]
		public void Can_Read_values_when_two_indexes_exist()
		{
			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @" 
	from doc in docs
	where doc.type == ""page""
	select new { doc.other};
"
						});
			db.PutIndex("pagesByTitle2",
					   new IndexDefinition
					   {
						   Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { doc.some };
"
					   });
			db.Put("1", Guid.Empty,
			       RavenJObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
				   new RavenJObject(), null);


			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Query = "some:val",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
		}

		[Fact]
		public void Updating_an_index_will_result_in_new_values()
		{
			db.PutIndex("pagesByTitle",
					   new IndexDefinition
					   {
						   Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { doc.other};
"
					   });
			db.PutIndex("pagesByTitle",
					   new IndexDefinition
					   {
						   Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { doc.other };
"
					   });
			db.Put("1", Guid.Empty,
			       RavenJObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
				   new RavenJObject(), null);


			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle", new IndexQuery
				{
					Query = "other:var",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
		}

		[Fact]
		public void Can_read_values_from_index_of_documents_already_in_db()
		{
			db.Put("1", Guid.Empty,
			       RavenJObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
				   new RavenJObject(), null);

			db.PutIndex("pagesByTitle",
					   new IndexDefinition
					   {
						   Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { doc.other };
"
					   });
			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle", new IndexQuery
				{
					Query = "other:var",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
		}

		[Fact]
		public void Can_read_values_from_indexes_of_documents_already_in_db_when_multiple_docs_exists()
		{
			db.Put(null, Guid.Empty,
			       RavenJObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
				   new RavenJObject(), null);
			db.Put(null, Guid.Empty,
			       RavenJObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
				   new RavenJObject(), null);

			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { doc.other };
"
						});
			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle", new IndexQuery
				{
					Query = "other:var",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(2, docs.Results.Count);
		}

		[Fact]
		public void Can_query_by_stop_words()
		{
			db.PutIndex("regionIndex", new IndexDefinition
			{
				Map = @"
					from doc in docs 
					select new { doc.Region };
					",
				Indexes = {{"Region", FieldIndexing.NotAnalyzed}}
			});

			db.Put("1", Guid.Empty, RavenJObject.Parse(
			@"{
				Region: 'A', 
			}"),
			new RavenJObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("regionIndex", new IndexQuery
				{
					Query = "Region:[[A]]",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);
			Assert.Equal(1, docs.Results.Count);
		}

	}
}
