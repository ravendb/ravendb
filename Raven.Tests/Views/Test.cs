namespace Raven.Tests.Views
{
    using Raven.Database.Linq;
    using System.Linq;
    using System;
    using Raven.Database.Linq.PrivateExtensions;
    public class MapReduceTest : AbstractViewGenerator
    {
        public MapReduceTest()
        {
            this.ViewText = @"from post in docs
select new {
  post.blog_id, 
  comments_length = post.comments.Length 
  }

from agg in results
group agg by agg.blog_id into g
select new { 
  blog_id = g.Key, 
  comments_length = g.Sum(x=>x.comments_length) 
  }";
            this.MapDefinition = docs => from post in docs
                                         select new { post.blog_id, comments_length = post.comments.Length, __document_id = post.__document_id };
            this.ReduceDefinition = results => from agg in results
                                               group agg by agg.blog_id into g
                                               select new { blog_id = g.Key, comments_length = g.Sum(x => x.comments_length) };
        }
    }

}