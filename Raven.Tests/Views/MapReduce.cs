//-----------------------------------------------------------------------
// <copyright file="MapReduce.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Views
{
	public class MapReduce : AbstractDocumentStorageTest
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
  comments_length = g.Sum(x=>(int)x.comments_length).ToString()
  }";

		private readonly DocumentDatabase db;

		public MapReduce()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
			db.PutIndex("CommentsCountPerBlog", new IndexDefinition{Map = map, Reduce = reduce, Indexes = {{"blog_id", FieldIndexing.NotAnalyzed}}});
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
		    Assert.Equal(@"{""blog_id"":""3"",""comments_length"":""14""}", q.Results[0].ToString(Formatting.None));
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

			GetUnstableQueryResult("blog_id:3");
		    

			db.Put("docs/0", null, RavenJObject.Parse("{blog_id: 3, comments: [{}]}"), new RavenJObject(), null);

            var q = GetUnstableQueryResult("blog_id:3");
		    
			Assert.Equal(@"{""blog_id"":""3"",""comments_length"":""12""}", q.Results[0].ToString(Formatting.None));
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
		    
			Assert.Equal(@"{""blog_id"":""3"",""comments_length"":""11""}", q.Results[0].ToString(Formatting.None));
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
            Assert.Equal(@"{""blog_id"":""3"",""comments_length"":""11""}", q.Results[0].ToString(Formatting.None));
		}
	}
}
