using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Views
{
    public class MapReduce_IndependentSteps : AbstractDocumentStorageTest
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


        public MapReduce_IndependentSteps()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
			db.PutIndex("CommentsCountPerBlog", new IndexDefinition{Map = map, Reduce = reduce, Indexes = {{"blog_id", FieldIndexing.NotAnalyzed}}});
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
                db.Put("docs/" + i, null, JObject.Parse(values[i]), new JObject(), null);
            }

            db.SpinBackgroundWorkers();

            QueryResult q = null;
            for (var i = 0; i < 5; i++)
            {
                do
                {
                    q = db.Query("CommentsCountPerBlog", new IndexQuery
                    {
                        Query = "blog_id:3",
                        Start = 0,
                        PageSize = 10
                    });
                    Thread.Sleep(100);
                } while (q.IsStale);
            }
            Assert.Equal(@"{""blog_id"":""3"",""comments_length"":""14""}", q.Results[0].ToString(Formatting.None));
        }

    }
}