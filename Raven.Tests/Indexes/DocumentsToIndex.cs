using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class DocumentsToIndex : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public DocumentsToIndex()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
			db.SpinBackgroundWorkers();
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

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
			       JObject.Parse(
			       	@"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 5,
                '@metadata': {'@id': 1}
            }"),
                   new JObject(), null);

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
			Assert.Equal(1, docs.Results.Length);
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
				JObject.Parse(
					"{'name':'ayende','email':'ayende@ayende.com','projects':[{'name':'raven'}], '@metadata': { '@id': 1}}");
            db.Put("1", Guid.Empty, document, new JObject(), null);

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
			Assert.Equal(1, docs.Results.Length);
			var jProperty = docs.Results[0].Property("name");
			Assert.Equal("ayende", jProperty.Value.Value<string>());
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
				JObject.Parse(
					"{'name':'ayende','email':'ayende@ayende.com','projects':[{'name':'raven', 'num': 5}, {'name':'crow', 'num': 6}], '@metadata': { '@id': 1}}");
            db.Put("1", Guid.Empty, document, new JObject(), null);

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
			Assert.Equal(0, docs.Results.Length);
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
			       JObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
                   new JObject(), null);


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
			Assert.Equal(1, docs.Results.Length);
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
			       JObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
                   new JObject(), null);


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
			Assert.Equal(1, docs.Results.Length);
		}

		[Fact]
		public void Can_read_values_from_index_of_documents_already_in_db()
		{
			db.Put("1", Guid.Empty,
			       JObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
                   new JObject(), null);

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
			Assert.Equal(1, docs.Results.Length);
		}

		[Fact]
		public void Can_read_values_from_indexes_of_documents_already_in_db_when_multiple_docs_exists()
		{
			db.Put(null, Guid.Empty,
			       JObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
                   new JObject(), null);
			db.Put(null, Guid.Empty,
			       JObject.Parse(
			       	"{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"),
                   new JObject(), null);

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
			Assert.Equal(2, docs.Results.Length);
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

            db.Put("1", Guid.Empty, JObject.Parse(
            @"{
                Region: 'A', 
            }"),
            new JObject(), null);

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
            Assert.Equal(1, docs.Results.Length);
        }

    }
}