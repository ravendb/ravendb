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
                Assert.Equal(100, vals.Counters.Count);

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
                        c.Increment(name);
                    }

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, counterNames));
                Assert.Equal(1000, vals.Counters.Count);

            }

        }

        [Fact]
        public void GetDocumentsCommandShouldDiscardNullIds()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/2";
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
                    Assert.Equal(100, command.Result.Results.Length);
                }

            }
        }

        [Fact]
        public void GetDocumentsCommandShouldDiscardNullIds_PostGet()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/2";
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
                    Assert.Equal(1000, command.Result.Results.Length);
                }

            }
        }
    }
}
