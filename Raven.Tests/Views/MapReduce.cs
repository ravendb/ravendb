//-----------------------------------------------------------------------
// <copyright file="MapReduce.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Client.Embedded;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Database;
using Xunit;
using System.Linq;

namespace Raven.Tests.Views
{
	public class MapReduce : RavenTest
	{
		private const string map =
			@"from post in docs
select new {
  post.blog_id, 
  comments_length = post.comments.Length 
  }";

		private const string reduce =
			@"
from agg in results
group agg by agg.blog_id into g
select new { 
  blog_id = g.Key, 
  comments_length = g.Sum(x=>(int)x.comments_length)
  }";

		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public MapReduce()
		{
			store = NewDocumentStore(runInMemory: true);
			db = store.DocumentDatabase;
			db.PutIndex("CommentsCountPerBlog", new IndexDefinition{Map = map, Reduce = reduce, Indexes = {{"blog_id", FieldIndexing.NotAnalyzed}}});
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanGetReducedValues()
		{
		    var values = new[]
		    {
		        "{blog_id: 3, comments: [{},{},{}]}",
		        "{blog_id: 5, comments: [{},{},{},{}]}",
		        "{blog_id: 6, comments: [{},{},{},{},{},{}]}",
		        "{blog_id: 7, comments: [{}]}",
		        "{blog_id: 3, comments: [{},{},{}]}",
		        "{blog_id: 3, comments: [{},{},{},{},{}]}",
		        "{blog_id: 2, comments: [{},{},{},{},{},{},{},{}]}",
		        "{blog_id: 4, comments: [{},{},{}]}",
		        "{blog_id: 5, comments: [{},{}]}",
		        "{blog_id: 3, comments: [{},{},{}]}",
		        "{blog_id: 5, comments: [{}]}",
		    };
		    for (int i = 0; i < values.Length; i++)
		    {
		        db.Put("docs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject(), null);
		    }

		    var q = GetUnstableQueryResult("blog_id:3");
		    Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString(Formatting.None));
		}

		//issue --> indice name : scheduled_reductions_by_view -->multi-tree key commentscountperblog
		[Fact]
		public void DoesNotOverReduce() 
		{
			db.Configuration.MaxNumberOfItemsToReduceInSingleBatch = 512;
			for (int i = 0; i < 1024; i++) {
				db.Put("docs/" + i, null, RavenJObject.Parse("{blog_id: " + i + ", comments: [{},{},{}]}"), new RavenJObject(), null);
			}

			var q = GetUnstableQueryResult("blog_id:3");
			Assert.False(q.IsStale);
			Assert.Equal(@"{""blog_id"":3,""comments_length"":3}", q.Results[0].ToString(Formatting.None));

            var index = db.Statistics.Indexes.First(x => x.PublicName == "CommentsCountPerBlog");
			// we add 100 because we might have reduces running in the middle of the operation
			Assert.True((1024 + 100) >= index.ReduceIndexingAttempts,
				"1024 + 100 >= " + index.ReduceIndexingAttempts + " failed");
		}

	    private QueryResult GetUnstableQueryResult(string query)
	    {
	        int count = 0;
	        QueryResult q = null;
	        do
	        {
	            q = db.Query("CommentsCountPerBlog", new IndexQuery
	            {
	                Query = query,
	                Start = 0,
	                PageSize = 10
	            });
	            if (q.IsStale)
	                Thread.Sleep(100);
	        } while (q.IsStale && count++ < 100);
	    	foreach (var result in q.Results)
	    	{
	    		result.Remove("@metadata");
	    	}
	        return q;
	    }

	    [Fact]
		public void CanUpdateReduceValue()
		{
			var values = new[]
			{
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{},{},{}]}",
				"{blog_id: 6, comments: [{},{},{},{},{},{}]}",
				"{blog_id: 7, comments: [{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 3, comments: [{},{},{},{},{}]}",
				"{blog_id: 2, comments: [{},{},{},{},{},{},{},{}]}",
				"{blog_id: 4, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{}]}",
			};
			for (int i = 0; i < values.Length; i++)
			{
				db.Put("docs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject(), null);
			}

			var q = GetUnstableQueryResult("blog_id:3");

			Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString(Formatting.None));
			
			db.Put("docs/0", null, RavenJObject.Parse("{blog_id: 3, comments: [{}]}"), new RavenJObject(), null);

			q = GetUnstableQueryResult("blog_id:3");
		    
			Assert.Equal(@"{""blog_id"":3,""comments_length"":12}", q.Results[0].ToString(Formatting.None));
		}


		[Fact]
		public void CanDelete()
		{
			var values = new[]
			{
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{},{},{}]}",
				"{blog_id: 6, comments: [{},{},{},{},{},{}]}",
				"{blog_id: 7, comments: [{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 3, comments: [{},{},{},{},{}]}",
				"{blog_id: 2, comments: [{},{},{},{},{},{},{},{}]}",
				"{blog_id: 4, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{}]}",
			};
			for (int i = 0; i < values.Length; i++)
			{
				db.Put("docs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject(), null);
			}

			GetUnstableQueryResult("blog_id:3");
		    

			db.Delete("docs/0", null, null);

			var q = GetUnstableQueryResult("blog_id:3");
		    
			Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString(Formatting.None));
		}

		[Fact]
		public void CanUpdateReduceValue_WhenChangingReduceKey()
		{
			var values = new[]
			{
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{},{},{}]}",
				"{blog_id: 6, comments: [{},{},{},{},{},{}]}",
				"{blog_id: 7, comments: [{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 3, comments: [{},{},{},{},{}]}",
				"{blog_id: 2, comments: [{},{},{},{},{},{},{},{}]}",
				"{blog_id: 4, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{},{}]}",
				"{blog_id: 3, comments: [{},{},{}]}",
				"{blog_id: 5, comments: [{}]}",
			};
			for (int i = 0; i < values.Length; i++)
			{
				db.Put("docs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject(), null);
			}

			GetUnstableQueryResult("blog_id:3");
		    
			db.Put("docs/0", null, RavenJObject.Parse("{blog_id: 7, comments: [{}]}"), new RavenJObject(), null);

			var q = GetUnstableQueryResult("blog_id:3");
			Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString(Formatting.None));
		}
	}
}
