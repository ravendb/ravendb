using System;
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

        Assert.Equal($"Index '{index.IndexName}' contains a lot of `let` clause.", performanceHint.Item2["Title"]);

        do
        {
            performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
        } while (performanceHint.Item2 != null && performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

        if (performanceHint.Item2 != null)
            Assert.NotEqual($"Index '{index.IndexName}' contains a lot of `let` clause.", performanceHint.Item2["Title"]);
    }

    [Fact]
    public Task QuerySyntax() => AssertPerformanceHint<QuerySyntaxIndex>();

    [Fact]
    public Task MethodSyntax() => AssertPerformanceHint<MethodSyntaxIndex>();
    
    [Fact]
    public Task BigStackTest() => AssertPerformanceHint<MaxStackTest>();
    
    private class ExampleDocument
    {
        public string Name { get; set; }
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
    
      private class MaxStackTest : AbstractIndexCreationTask<ExampleDocument>
    {
        public MaxStackTest()
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
                    let a33 = 33
                    let a34 = 34
                    let a35 = 35
                    let a36 = 36
                    let a37 = 37
                    let a38 = 38
                    let a39 = 39
                    let a40 = 40
                    let a41 = 41
                    let a42 = 42
                    let a43 = 43
                    let a44 = 44
                    let a45 = 45
                    let a46 = 46
                    let a47 = 47
                    let a48 = 48
                    let a49 = 49
                    let a50 = 50
                    let a51 = 51
                    let a52 = 52
                    let a53 = 53
                    let a54 = 54
                    let a55 = 55
                    let a56 = 56
                    let a57 = 57
                    let a58 = 58
                    let a59 = 59
                    let a60 = 60
                    let a61 = 61
                    let a62 = 62
                    let a63 = 63
                    let a64 = 64
                    select new {
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
                        A33 = a33,
                        A34 = a34,
                        A35 = a35,
                        A36 = a36,
                        A37 = a37,
                        A38 = a38,
                        A39 = a39,
                        A40 = a40,
                        A41 = a41,
                        A42 = a42,
                        A43 = a43,
                        A44 = a44,
                        A45 = a45,
                        A46 = a46,
                        A47 = a47,
                        A48 = a48,
                        A49 = a49,
                        A50 = a50,
                        A51 = a51,
                        A52 = a52,
                        A53 = a53,
                        A54 = a54,
                        A55 = a55,
                        A56 = a56,
                        A57 = a57,
                        A58 = a58,
                        A59 = a59,
                        A60 = a60,
                        A61 = a61,
                        A62 = a62,
                        A63 = a63,
                        A64 = a64,
                        Field = doc.Name
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
