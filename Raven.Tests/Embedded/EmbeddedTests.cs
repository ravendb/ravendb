using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common;
using Raven.Tests.Document;
using Raven.Tests.Helpers;
using Raven.Tests.Notifications;
using Xunit;

namespace Raven.Tests.Embedded
{
    public class EmbeddedTests : RavenTest
    {
        [Fact]
        public void Can_get_documents()
        {
            using (var server = GetNewServer())
            {
                using (var session = server.DocumentStore.OpenSession())
                {
                    session.Store(new Company {Name = "Company A", Id = "1"});
                    session.Store(new Company {Name = "Company B", Id = "2"});
                    session.SaveChanges();
                }
                JsonDocument[] jsonDocuments = server.DocumentStore.DatabaseCommands.GetDocuments(0, 10, true);
                Assert.Equal(3, jsonDocuments.Length);
            }
        }

        [Fact]
        public void Can_receive_changes_notification()
        {
            using (var server = GetNewServer())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = server.DocumentStore.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForDocument("items/1");
                observableWithTask.Task.Wait();
                observableWithTask.Subscribe(list.Add);

                using (var session = server.DocumentStore.OpenSession())
                {
                    session.Store(new ClientServer.Item(), "items/1");
                    session.SaveChanges();
                }

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(3)));
            }
        }

        [Fact]
        public void Streaming_Results_Should_Sort_Properly()
        {
            using (var server = GetNewServer())
            {
                var documentStore = server.DocumentStore;
                documentStore.ExecuteIndex(new FooIndex());

                using (var session = documentStore.OpenSession())
                {
                    var random = new System.Random();

                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Foo {Num = random.Next(1, 100)});
                    }
                    session.SaveChanges();
                }
                WaitForIndexing(documentStore);

                Foo last = null;

                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<Foo, FooIndex>().OrderBy(x => x.Num);
                    var enumerator = session.Advanced.Stream(q);

                    while (enumerator.MoveNext())
                    {
                        var foo = enumerator.Current.Document;
                        Debug.WriteLine("{0} - {1}", foo.Id, foo.Num);

                        if (last != null)
                        {
                            // If the sort worked, this test should pass
                            Assert.True(last.Num <= foo.Num);
                        }

                        last = foo;
                    }
                }
            }
        }

        [Fact]
        public void CanInsertSeveralDocuments()
        {
            using (var server = GetNewServer())
            {
                var store = server.DocumentStore;
                var bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions(), (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes());
                bulkInsertOperation.Write("one", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Write("two", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Dispose();

                Assert.Equal("passed", store.DatabaseCommands.Get("one").DataAsJson.Value<string>("test"));
                Assert.Equal("passed", store.DatabaseCommands.Get("two").DataAsJson.Value<string>("test"));
            }
        }

        public class Foo
        {
            public string Id { get; set; }
            public int Num { get; set; }
        }

        public class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = foos => from foo in foos
                              select new { foo.Num };

                Sort(x => x.Num, SortOptions.Int);
            }
        }
    }
}
