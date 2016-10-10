//-----------------------------------------------------------------------
// <copyright file="MapReduce.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Document;
using Raven.Client.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Tests.Views
{
    public class MapReduce : RavenTestBase
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

            using (var store = GetDocumentStore())
            {
                Fill(store);

                for (int i = 0; i < values.Length; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                var q = GetUnstableQueryResult(store, "blog_id:3");
                Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString(Formatting.None));
            }
        }

        //issue --> indice name : scheduled_reductions_by_view -->multi-tree key commentscountperblog
        [Fact]
        public void DoesNotOverReduce()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                for (int i = 0; i < 1024; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse("{blog_id: " + i + ", comments: [{},{},{}]}"), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                WaitForIndexing(store);

                var index = store.DatabaseCommands.GetIndexStatistics("CommentsCountPerBlog");
                // we add 100 because we might have reduces running in the middle of the operation
                Assert.True((1024 + 100) >= index.ReduceAttempts.Value,
                    "1024 + 100 >= " + index.ReduceAttempts + " failed");
            }
        }

        private static QueryResult GetUnstableQueryResult(IDocumentStore store, string query)
        {
            int count = 0;
            QueryResult q;
            do
            {
                q = store.DatabaseCommands.Query("CommentsCountPerBlog", new IndexQuery
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

            using (var store = GetDocumentStore())
            {
                Fill(store);

                for (int i = 0; i < values.Length; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                var q = GetUnstableQueryResult(store, "blog_id:3");

                Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString(Formatting.None));

                store.DatabaseCommands.Put("blogs/0", null, RavenJObject.Parse("{blog_id: 3, comments: [{}]}"), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });

                q = GetUnstableQueryResult(store, "blog_id:3");

                Assert.Equal(@"{""blog_id"":3,""comments_length"":12}", q.Results[0].ToString(Formatting.None));
            }
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

            using (var store = GetDocumentStore())
            {
                Fill(store);

                for (int i = 0; i < values.Length; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                GetUnstableQueryResult(store, "blog_id:3");

                store.DatabaseCommands.Delete("blogs/0", null);

                var q = GetUnstableQueryResult(store, "blog_id:3");

                Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString(Formatting.None));
            }
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

            using (var store = GetDocumentStore())
            {
                Fill(store);

                for (int i = 0; i < values.Length; i++)
                {
                    store.DatabaseCommands.Put("blogs/" + i, null, RavenJObject.Parse(values[i]), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });
                }

                GetUnstableQueryResult(store, "blog_id:3");

                store.DatabaseCommands.Put("blogs/0", null, RavenJObject.Parse("{blog_id: 7, comments: [{}]}"), new RavenJObject { { "Raven-Entity-Name", "Blogs" } });

                var q = GetUnstableQueryResult(store, "blog_id:3");
                Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString(Formatting.None));
            }
        }
    }
}
