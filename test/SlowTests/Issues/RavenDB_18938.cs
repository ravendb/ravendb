using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18938 : RavenTestBase
{
    public RavenDB_18938(ITestOutputHelper output) : base(output)
    {
    }

    private async Task AssertPerformanceHint<TIndex>()
        where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);
        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
        using var _ = db.NotificationCenter.TrackActions(notificationsQueue, null);

        {
            using var session = store.OpenSession();
            session.Store(new ExampleDocument() {Name = "Maciej"});
            session.Store(new ExampleDocument() {Name = "Gracjan"});
            session.Store(new ExampleDocument() {Name = "Marcin"});
            session.SaveChanges();
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);

        Tuple<bool, DynamicJsonValue> performanceHint;

        do
        {
            performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
        } while (performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

        Assert.Equal($"Index '{index.IndexName}' contains a lot of `let` clauses. Index contains 33 `let` clauses but we suggest not to exceed 32.", performanceHint.Item2["Title"]);
        WaitForUserToContinueTheTest(store);
        do
        {
            performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
        } while (performanceHint.Item2 != null && performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

        if (performanceHint.Item2 != null)
            Assert.NotEqual($"Index '{index.IndexName}' contains a lot of `let` clauses. Index contains 33 `let` clauses but we suggest not to exceed 32.", performanceHint.Item2["Title"]);
    }

    [Fact]
    public Task QuerySyntax() => AssertPerformanceHint<QuerySyntaxIndex>();

    [Fact]
    public Task QuerySyntaxReduce() => AssertPerformanceHint<QuerySyntaxMapReduceIndex>();
    
    [Fact]
    public Task MethodSyntax() => AssertPerformanceHint<MethodSyntaxIndex>();
    
    [Fact]
    public Task MethodSyntaxReduce() => AssertPerformanceHint<MethodSyntaxMapReduceIndex>();

    private class ExampleDocument
    {
        public string Name { get; set; }
    }

    private class MethodSyntaxMapReduceIndex : AbstractIndexCreationTask<ExampleDocument>
    {
        public MethodSyntaxMapReduceIndex()
        {
            Map = documents => from doc in documents
                select new
                {
                    Name = doc.Name,
                    Count = 1
                };

            Reduce = results =>
                from result in results
                group result by result.Name
                into g
                let a0 = 0
                let a1 = 1
                let a2 = 2
                let a3 = 3
                let a4 = 4
                let a5 = 5
                let a6 = 6
                let a7 = 7
                let a8 = 8
                let a9 = 9
                let a10 = 10
                let a11 = 11
                let a12 = 12
                let a13 = 13
                let a14 = 14
                let a15 = 15
                let a16 = 16
                let a17 = 17
                let a18 = 18
                let a19 = 19
                let a20 = 20
                let a21 = 21
                let a22 = 22
                let a23 = 23
                let a24 = 24
                let a25 = 25
                let a26 = 26
                let a27 = 27
                let a28 = 28
                let a29 = 29
                let a30 = 30
                let a31 = 31
                let a32 = 32
                
                select new {Name = g.Key, Count = a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21 + a22 + a23 + a24 + a25 + a26 + a27 + a28 + a29 + a30 + a31 +  a32};
        }
    }
    
    private class MethodSyntaxIndex : AbstractIndexCreationTask<ExampleDocument>
    {
        public MethodSyntaxIndex()
        {
            Map = documents => from doc in documents
                let a0 = 0
                let a1 = 1
                let a2 = 2
                let a3 = 3
                let a4 = 4
                let a5 = 5
                let a6 = 6
                let a7 = 7
                let a8 = 8
                let a9 = 9
                let a10 = 10
                let a11 = 11
                let a12 = 12
                let a13 = 13
                let a14 = 14
                let a15 = 15
                let a16 = 16
                let a17 = 17
                let a18 = 18
                let a19 = 19
                let a20 = 20
                let a21 = 21
                let a22 = 22
                let a23 = 23
                let a24 = 24
                let a25 = 25
                let a26 = 26
                let a27 = 27
                let a28 = 28
                let a29 = 29
                let a30 = 30
                let a31 = 31
                let a32 = 32
                select new
                {
                    A0 = a0,
                    A1 = a1,
                    A2 = a2,
                    A3 = a3,
                    A4 = a4,
                    A5 = a5,
                    A6 = a6,
                    A7 = a7,
                    A8 = a8,
                    A9 = a9,
                    A10 = a10,
                    A11 = a11,
                    A12 = a12,
                    A13 = a13,
                    A14 = a14,
                    A15 = a15,
                    A16 = a16,
                    A17 = a17,
                    A18 = a18,
                    A19 = a19,
                    A20 = a20,
                    A21 = a21,
                    A22 = a22,
                    A23 = a23,
                    A24 = a24,
                    A25 = a25,
                    A26 = a26,
                    A27 = a27,
                    A28 = a28,
                    A29 = a29,
                    A30 = a30,
                    A31 = a31,
                    A32 = a32,
                    Field = doc.Name
                };
        }
    }
    
    private class QuerySyntaxMapReduceIndex : AbstractIndexCreationTask
    {
        public QuerySyntaxMapReduceIndex()
        {
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition()
            {
                Maps =
                {
                    @"from documentItself in docs.ExampleDocuments
                    select new
                    {
                        Name = documentItself.Name,
                        Count = 1
                    };",
                },
                Reduce =
                    @"from result in results
                        group result by result.Name into g
                        let a0 = 0
                        let a1 = 1
                        let a2 = 2
                        let a3 = 3
                        let a4 = 4
                        let a5 = 5
                        let a6 = 6
                        let a7 = 7
                        let a8 = 8
                        let a9 = 9
                        let a10 = 10
                        let a11 = 11
                        let a12 = 12
                        let a13 = 13
                        let a14 = 14
                        let a15 = 15
                        let a16 = 16
                        let a17 = 17
                        let a18 = 18
                        let a19 = 19
                        let a20 = 20
                        let a21 = 21
                        let a22 = 22
                        let a23 = 23
                        let a24 = 24
                        let a25 = 25
                        let a26 = 26
                        let a27 = 27
                        let a28 = 28
                        let a29 = 29
                        let a30 = 30
                        let a31 = 31
                        let a32 = 32
                        select new 
                        {
                            Name = g.Key, 
                            Count = a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21 + a22 + a23 + a24 + a25 + a26 + a27 + a28 + a29 + a30 + a31 +  a32
                        };"
                
            };
        }
    }

    private class QuerySyntaxIndex : AbstractIndexCreationTask
    {
        public QuerySyntaxIndex()
        {
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition()
            {
                Maps =
                {
                    @"from documentItself in docs.ExampleDocuments
                let a0 = 0
                let a1 = 1
                let a2 = 2
                let a3 = 3
                let a4 = 4
                let a5 = 5
                let a6 = 6
                let a7 = 7
                let a8 = 8
                let a9 = 9
                let a10 = 10
                let a11 = 11
                let a12 = 12
                let a13 = 13
                let a14 = 14
                let a15 = 15
                let a16 = 16
                let a17 = 17
                let a18 = 18
                let a19 = 19
                let a20 = 20
                let a21 = 21
                let a22 = 22
                let a23 = 23
                let a24 = 24
                let a25 = 25
                let a26 = 26
                let a27 = 27
                let a28 = 28
                let a29 = 29
                let a30 = 30
                let a31 = 31
                let a32 = 32
                select new
                {
                    A0 = a0,
                    A1 = a1,
                    A2 = a2,
                    A3 = a3,
                    A4 = a4,
                    A5 = a5,
                    A6 = a6,
                    A7 = a7,
                    A8 = a8,
                    A9 = a9,
                    A10 = a10,
                    A11 = a11,
                    A12 = a12,
                    A13 = a13,
                    A14 = a14,
                    A15 = a15,
                    A16 = a16,
                    A17 = a17,
                    A18 = a18,
                    A19 = a19,
                    A20 = a20,
                    A21 = a21,
                    A22 = a22,
                    A23 = a23,
                    A24 = a24,
                    A25 = a25,
                    A26 = a26,
                    A27 = a27,
                    A28 = a28,
                    A29 = a29,
                    A30 = a30,
                    A31 = a31,
                    A32 = a32,
                    Field = documentItself.Name
                };"
                }
            };
        }
    }
}
