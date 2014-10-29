using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Issues
{
    public class RavenDB1508 : RavenTest
    {
        public class Foo
        {
            public string Name { get; set; }
            public Bar[] Bars { get; set; }
        }

        public class Bar
        {
            public float Number { get; set; }
        }

        public class BarViewModel
        {
            public string FooName { get; set; }
            public float Number { get; set; }
        }

        class BarSearchIndex : AbstractIndexCreationTask<Foo, BarViewModel>
        {
            public BarSearchIndex()
            {
                Map = (foos) => from f in foos
                                from b in f.Bars
                                select new BarViewModel() { FooName = f.Name, Number = b.Number };

                StoreAllFields(Raven.Abstractions.Indexing.FieldStorage.Yes);
                Sort(x=>x.Number, SortOptions.Float);
            }
        }

        [Fact]
        public void RangeQuerySucceedsWithEmbeddableStore()
        {
            using (var store = NewDocumentStore())
            {
                VerifyRangeQueryWithStore(store);
            }
        }

        [Fact]
        public void RangeQueryFailsAgainstServer()
        {
            using (var store = NewRemoteDocumentStore())
            {
                VerifyRangeQueryWithStore(store);
            }
        }


        void VerifyRangeQueryWithStore(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                store.ExecuteIndex(new BarSearchIndex());

                session.Store(new Foo() { Name = "a", Bars = new Bar[] { new Bar() { Number = 1.0f }, new Bar() { Number = 2.0f } } });
                session.Store(new Foo() { Name = "b", Bars = new Bar[] { new Bar() { Number = 3.0f } } });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                AssertEventually(() =>
                {
                    var results = session.Query<BarViewModel, BarSearchIndex>()
                        .Where(x => x.Number >= 1.9f && x.Number <= 2.1f)
                        .ProjectFromIndexFieldsInto<BarViewModel>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .ToList();

                    var model = Assert.Single(results);
                    Assert.Equal(2.0f, model.Number);
                }, TimeSpan.FromSeconds(10));
            }
        }
        public static void AssertEventually(Action assertion, TimeSpan timeout)
        {
            DateTime start = DateTime.Now;
            TimeSpan retry = TimeSpan.FromMilliseconds(10);

            while (true)
            {
                try
                {
                    assertion();
                    return;
                }
                catch (AssertException ex)
                {
                    var elapsed = DateTime.Now - start;
                    if (elapsed > timeout)
                        throw new AggregateException("Assertions timed out", ex);

                    Thread.Sleep(retry);
                    retry = retry + retry; // double the retry period
                }
            }
        }
    }
}