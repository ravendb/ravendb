//-----------------------------------------------------------------------
// <copyright file="MapReduce.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
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
                    for (int i = 0; i < values.Length; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(values[i])))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    var q = GetUnstableQueryResult(store, commands, "blog_id = 3");
                    Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString());
                }
            }
        }

        //issue --> indice name : scheduled_reductions_by_view -->multi-tree key commentscountperblog
        [Fact]
        public void DoesNotOverReduce()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var commands = store.Commands())
                {
                    for (int i = 0; i < 1024; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("{'blog_id': " + i + ", 'comments': [{},{},{}]}")))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    WaitForIndexing(store);

                    IndexStats stats = null;

                    Assert.True(SpinWait.SpinUntil(() =>
                    {
                        stats = store.Maintenance.Send(new GetIndexStatisticsOperation("CommentsCountPerBlog"));

                        return stats.ReduceAttempts != null;
                    }, TimeSpan.FromSeconds(5)));

                    // we add 100 because we might have reduces running in the middle of the operation
                    Assert.True((1024 + 100) >= stats.ReduceAttempts.Value,
                        "1024 + 100 >= " + stats.ReduceAttempts + " failed");
                }
            }
        }

        private QueryResult GetUnstableQueryResult(IDocumentStore store, DocumentStoreExtensions.DatabaseCommands commands, string query)
        {
            WaitForIndexing(store);

            var q = commands.Query(new IndexQuery()
            {
                Query = $"FROM INDEX 'CommentsCountPerBlog' WHERE {query} OFFSET 0 FETCH 10"
            });

            var array = new DynamicJsonArray();
            foreach (BlittableJsonReaderObject result in q.Results)
            {
                result.Modifications = new DynamicJsonValue(result);
                result.Modifications.Remove("@metadata");

                array.Add(commands.Context.ReadObject(result, "blog"));
            }

            var djv = new DynamicJsonValue
            {
                ["_"] = array
            };

            var json = commands.Context.ReadObject(djv, "blog");

            q.Results = (BlittableJsonReaderArray)json["_"];
            return q;
        }

        [Fact]
        public void CanUpdateReduceValue()
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
                    for (int i = 0; i < values.Length; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(values[i])))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    var q = GetUnstableQueryResult(store, commands, "blog_id = 3");

                    Assert.Equal(@"{""blog_id"":3,""comments_length"":14}", q.Results[0].ToString());

                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("{'blog_id': 3, 'comments': [{}]}")))
                    {
                        var json = commands.Context.ReadForMemory(stream, "blog");
                        commands.Put("blogs/0", null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                    }

                    q = GetUnstableQueryResult(store, commands, "blog_id = 3");

                    Assert.Equal(@"{""blog_id"":3,""comments_length"":12}", q.Results[0].ToString());
                }
            }
        }


        [Fact]
        public void CanDelete()
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
                    for (int i = 0; i < values.Length; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(values[i])))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    GetUnstableQueryResult(store, commands, "blog_id = 3");

                    commands.Delete("blogs/0", null);

                    var q = GetUnstableQueryResult(store, commands, "blog_id = 3");

                    Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString());
                }
            }
        }

        [Fact]
        public void CanUpdateReduceValue_WhenChangingReduceKey()
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
                    for (int i = 0; i < values.Length; i++)
                    {
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(values[i])))
                        {
                            var json = commands.Context.ReadForMemory(stream, "blog");
                            commands.Put("blogs/" + i, null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                        }
                    }

                    GetUnstableQueryResult(store, commands, "blog_id = 3");

                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("{'blog_id': 7, 'comments': [{}]}")))
                    {
                        var json = commands.Context.ReadForMemory(stream, "blog");
                        commands.Put("blogs/0", null, json, new Dictionary<string, object> { { "@collection", "Blogs" } });
                    }

                    var q = GetUnstableQueryResult(store, commands, "blog_id = 3");
                    Assert.Equal(@"{""blog_id"":3,""comments_length"":11}", q.Results[0].ToString());
                }
            }
        }
    }
}
