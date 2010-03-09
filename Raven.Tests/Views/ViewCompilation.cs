using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using Raven.Database.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Views
{
    public class ViewCompilation
    {
        private readonly IEnumerable<ExpandoObject> source;

        public ViewCompilation()
        {
            source = ConvertToExpando(new[]
            {
                new {blog_id = 3, comments = new object[3], __document_id = 1},
                new {blog_id = 5, comments = new object[4], __document_id = 1},
                new {blog_id = 6, comments = new object[6], __document_id = 1},
                new {blog_id = 7, comments = new object[1], __document_id = 1},
                new {blog_id = 3, comments = new object[3], __document_id = 1},
                new {blog_id = 3, comments = new object[5], __document_id = 1},
                new {blog_id = 2, comments = new object[8], __document_id = 1},
                new {blog_id = 4, comments = new object[3], __document_id = 1},
                new {blog_id = 5, comments = new object[2], __document_id = 1},
                new {blog_id = 3, comments = new object[3], __document_id = 1},
                new {blog_id = 5, comments = new object[1], __document_id = 1}
            });
        }

        private const string map =
            @"from post in docs
select new {
  post.blog_id, 
  comments_length = post.comments.Length 
  }";

        private const string reduce = @"
from agg in results
group agg by agg.blog_id into g
select new { 
  blog_id = g.Key, 
  comments_length = g.Sum(x=>x.comments_length) 
  }";
        [Fact]
        public void CanDetectGroupByTarget()
        {
            var abstractViewGenerator = new DynamicViewCompiler("test", map, reduce).GenerateInstance();
            Assert.Equal("blog_id", abstractViewGenerator.GroupByField);
        }

        [Fact]
        public void CanCompileQuery()
        {
            var abstractViewGenerator = new DynamicViewCompiler("test", map, reduce).GenerateInstance();
            Assert.NotNull(abstractViewGenerator);
        }

        [Fact]
        public void CanExecuteQuery()
        {
            var dynamicViewCompiler = new DynamicViewCompiler("test", map, reduce);
            var abstractViewGenerator = dynamicViewCompiler.GenerateInstance();
            var mapResults = abstractViewGenerator.MapDefinition(source).ToArray();
            var results = abstractViewGenerator.ReduceDefinition(mapResults).ToArray();
            Assert.Equal("{ blog_id = 3, comments_length = 14 }", results[0].ToString());
            Assert.Equal("{ blog_id = 5, comments_length = 7 }", results[1].ToString());
            Assert.Equal("{ blog_id = 6, comments_length = 6 }", results[2].ToString());
            Assert.Equal("{ blog_id = 7, comments_length = 1 }", results[3].ToString());
            Assert.Equal("{ blog_id = 2, comments_length = 8 }", results[4].ToString());
            Assert.Equal("{ blog_id = 4, comments_length = 3 }", results[5].ToString());
        }

        private static IEnumerable<ExpandoObject> ConvertToExpando(object[] objects)
        {
            foreach (var obj in objects)
            {
                var expando = new ExpandoObject();
                foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(obj))
                {
                    ((IDictionary<string, object>) expando).Add(property.Name, property.GetValue(obj));
                }
                yield return expando;
            }
        }
    }
}