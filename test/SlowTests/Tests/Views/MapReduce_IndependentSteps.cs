//-----------------------------------------------------------------------
// <copyright file="MapReduce_IndependentSteps.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Tests.Views
{
    public class MapReduce_IndependentSteps : RavenTestBase
    {
        private const string Map =
            @"from post in docs.Blogs
select new {
  post.blog_id, 
  comments_length = post.comments.Length 
  }";

        private const string Reduce =
            @"
from agg in results
group agg by agg.blog_id into g
select new { 
  blog_id = g.Key, 
  comments_length = g.Sum(x=>(int)x.comments_length)
  }";

        private static void Fill(IDocumentStore store)
        {
            store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
            {
                Name = "CommentsCountPerBlog",
                Maps = { Map },
                Reduce = Reduce,
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "blog_id", new IndexFieldOptions { Indexing = FieldIndexing.Exact } }
                }
            }}));
        }

        [Fact]
        public void CanGetReducedValues()
        {
            var values = new[]
            {
                "{'blog_id': 3, 'comments': [{},{},{}]}",
                "{'blog_id': 5, 'comments': [{},{},{},{}]}",
                "{'blog_id': 6, 'comments': [{},{},{},{},{},{}]}",
                "{'blog_id': 7, 'comments': [{}]}",
                "{'blog_id': 3, 'comments': [{},{},{}]}",
                "{'blog_id': 3, 'comments': [{},{},{},{},{}]}",
                "{'blog_id': 2, 'comments': [{},{},{},{},{},{},{},{}]}",
                "{'blog_id': 4, 'comments': [{},{},{}]}",
                "{'blog_id': 5, 'comments': [{},{}]}",
                "{'blog_id': 3, 'comments': [{},{},{}]}",
                "{'blog_id': 5, 'comments': [{}]}",
            };

            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < values.Length; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(values[i])))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    WaitForIndexing(store);

                    var q = commands.Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'CommentsCountPerBlog' WHERE blog_id = 3 LIMIT 10 OFFSET 0"
                    });

                    var result = (BlittableJsonReaderObject)q.Results[0];
                    result.Modifications = new DynamicJsonValue(result);
                    result.Modifications.Remove("@metadata");

                    result = commands.Context.ReadObject(result, "blog");

                    Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", result.ToString());
                }
            }
        }

    }
}
