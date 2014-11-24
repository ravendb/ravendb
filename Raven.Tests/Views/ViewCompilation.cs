//-----------------------------------------------------------------------
// <copyright file="ViewCompilation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Database.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Views
{
	public class ViewCompilation : NoDisposalNeeded
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
  comments_length = g.Sum(x=>x.comments_length) 
  }";

		private readonly IEnumerable<DynamicJsonObject> source;

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

		[Fact]
		public void CanDetectGroupByTarget()
		{
			var abstractViewGenerator = new DynamicViewCompiler("test", new IndexDefinition { Map = map, Reduce = reduce },  ".").GenerateInstance();
			var expandoObject = new ExpandoObject();
			((IDictionary<string,object>)expandoObject).Add("blog_id","1");
			Assert.Equal("1", abstractViewGenerator.GroupByExtraction(expandoObject));
		}

		[Fact]
		public void CanCompileQuery()
		{
			var abstractViewGenerator = new DynamicViewCompiler("test", new IndexDefinition { Map = map, Reduce = reduce },  ".").GenerateInstance();
			Assert.NotNull(abstractViewGenerator);
		}

		[Fact]
		public void CanExecuteQuery()
		{
			var dynamicViewCompiler = new DynamicViewCompiler("test", new IndexDefinition { Map = map, Reduce = reduce },  ".");
			var abstractViewGenerator = dynamicViewCompiler.GenerateInstance();
			var mapResults = abstractViewGenerator.MapDefinitions[0](source).ToArray();
			var results = abstractViewGenerator.ReduceDefinition(mapResults).ToArray();
			Assert.Equal("{ blog_id = 3, comments_length = 14 }", results[0].ToString());
			Assert.Equal("{ blog_id = 5, comments_length = 7 }", results[1].ToString());
			Assert.Equal("{ blog_id = 6, comments_length = 6 }", results[2].ToString());
			Assert.Equal("{ blog_id = 7, comments_length = 1 }", results[3].ToString());
			Assert.Equal("{ blog_id = 2, comments_length = 8 }", results[4].ToString());
			Assert.Equal("{ blog_id = 4, comments_length = 3 }", results[5].ToString());
		}

		private static IEnumerable<DynamicJsonObject> ConvertToExpando(IEnumerable<object> objects)
		{
			return objects.Select(obj => new DynamicJsonObject(RavenJObject.FromObject(obj)));
		}
	}
}