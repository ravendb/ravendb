using System;
using System.Linq;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19625 : RavenTestBase
{
    public RavenDB_19625(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanQueryIndexFilteredByDateTime()
    {
        using (var store = GetDocumentStore())
        {
            store.ExecuteIndex(new QueryDateTime_Index());

            using (var session = store.OpenSession())
            {
                session.Store(new Post { Id = "posts/1", Date = DateTime.UtcNow.AddMinutes(-5) });

                session.SaveChanges();

                Indexes.WaitForIndexing(store);
                
                var res = session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                    .Where(x => x.Date < DateTime.UtcNow)
                    .ProjectInto<QueryDateTime_Index.Result>()
                    .ToList();

                Assert.NotEmpty(res);
            }
        }
    }

    private class Post
    {
        public string Id { get; set; }
        public DateTime? Date { get; set; }
    }

    private class QueryDateTime_Index : AbstractJavaScriptIndexCreationTask
    {
        public class Result
        {
            public DateTime? Date { get; set; }
        }

        public QueryDateTime_Index()
        {
            Maps = new HashSet<string>
            {
                @"map('Posts', p => {
                    return {
                        Date: p.Date 
                    };
                });"
            };
        }
    }
}
