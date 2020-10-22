using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Counters;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14919 : RavenTestBase
    {
        public RavenDB_14919(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetCountersOperationShouldDiscardNullCounters()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/2";
                string[] counterNames = new string[124];

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var c = session.CountersFor(docId);

                    for (int i = 0; i < 100; i++)
                    {
                        string name = $"likes{i}";
                        counterNames[i] = name;
                        c.Increment(name);
                    }

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, counterNames));
                Assert.Equal(101, vals.Counters.Count);

                for (int i = 0; i < 100; i++)
                {
                    Assert.Equal(1, vals.Counters[i].TotalValue);
                }

                Assert.Null(vals.Counters[^1]);

                // test with returnFullResults = true

                vals = store.Operations.Send(new GetCountersOperation(docId, counterNames, returnFullResults: true));
                Assert.Equal(101, vals.Counters.Count);

                for (int i = 0; i < 100; i++)
                {
                    Assert.Equal(1, vals.Counters[i].CounterValues.Count);
                }

                Assert.Null(vals.Counters[^1]);
            }
        }

        [Fact]
        public void GetCountersOperationShouldDiscardNullCounters_PostGet()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/2";
                string[] counterNames = new string[1024];

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var c = session.CountersFor(docId);

                    for (int i = 0; i < 1000; i++)
                    {
                        string name = $"likes{i}";
                        counterNames[i] = name;
                        c.Increment(name, i);
                    }

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, counterNames));
                Assert.Equal(1001, vals.Counters.Count);

                for (int i = 0; i < 1000; i++)
                {
                    Assert.Equal(i, vals.Counters[i].TotalValue);
                }

                Assert.Null(vals.Counters[^1]);

                // test with returnFullResults = true
                vals = store.Operations.Send(new GetCountersOperation(docId, counterNames, returnFullResults: true));
                Assert.Equal(1001, vals.Counters.Count);

                for (int i = 0; i < 1000; i++)
                {
                    Assert.Equal(1, vals.Counters[i].CounterValues.Count);
                    Assert.Equal(i, vals.Counters[i].TotalValue);
                }

                Assert.Null(vals.Counters[^1]);
            }

        }

        [Fact]
        public void GetDocumentsCommandShouldDiscardNullIds()
        {
            using (var store = GetDocumentStore())
            {
                string[] ids = new string[124];
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        string id = $"users/{i}";
                        ids[i] = id;
                        session.Store(new User(), id);
                    }

                    session.SaveChanges();
                }

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var command = new GetDocumentsCommand(ids, includes: null, metadataOnly: false);
                    re.Execute(command, context);
                    Assert.Equal(101, command.Result.Results.Length);
                    Assert.Null(command.Result.Results[^1]);
                }
            }
        }

        [Fact]
        public void GetDocumentsCommandShouldDiscardNullIds_PostGet()
        {
            using (var store = GetDocumentStore())
            {
                string[] ids = new string[1024];
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        string id = $"users/{i}";
                        ids[i] = id;
                        session.Store(new User(), id);
                    }

                    session.SaveChanges();
                }

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var command = new GetDocumentsCommand(ids, includes: null, metadataOnly: false);
                    re.Execute(command, context);
                    Assert.Equal(1001, command.Result.Results.Length);
                    Assert.Null(command.Result.Results[^1]);
                }

            }
        }
    }
}
