//-----------------------------------------------------------------------
// <copyright file="MapReduce_IndependentSteps.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
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
            store.DatabaseCommands.PutIndex("CommentsCountPerBlog", new IndexDefinition
            {
                Maps = { Map },
                Reduce = Reduce,
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "blog_id", new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed } }
                }
            });
        }

        [Fact]
        public async Task CanGetReducedValues()
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

            using (var store = await GetDocumentStore())
            {
                Fill(store);

                for (var i = 0; i < values.Length; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                WaitForIndexing(store);

                QueryResult q = store.DatabaseCommands.Query("CommentsCountPerBlog", new IndexQuery
                {
                    Query = "blog_id:3",
                    Start = 0,
                    PageSize = 10
                });

                q.Results[0].Remove("@metadata");
                Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString(Formatting.None));
            }
        }

    }
}
