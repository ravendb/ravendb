//-----------------------------------------------------------------------
// <copyright file="MapReduce_IndependentSteps.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Tests.Views
{
    public class MapReduce_IndependentSteps : RavenNewTestBase
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
            store.Admin.Send(new PutIndexOperation("CommentsCountPerBlog", new IndexDefinition
            {
                Maps = { Map },
                Reduce = Reduce,
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "blog_id", new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed } }
                }
            }));
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
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, string> { { "@collection", "Blogs" } });
                        }
                    }

                    WaitForIndexing(store);

                    var q = commands.Query("CommentsCountPerBlog", new IndexQuery(store.Conventions)
                    {
                        Query = "blog_id:3",
                        Start = 0,
                        PageSize = 10
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
