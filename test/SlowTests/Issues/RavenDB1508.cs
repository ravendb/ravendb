using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Sdk;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB1508 : RavenTestBase
    {
        public RavenDB1508(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Name { get; set; }
            public Bar[] Bars { get; set; }
        }

        private class Bar
        {
            public float Number { get; set; }
        }

        private class BarViewModel
        {
            public string FooName { get; set; }
            public float Number { get; set; }
        }

        private class BarSearchIndex : AbstractIndexCreationTask<Foo, BarViewModel>
        {
            public BarSearchIndex()
            {
                Map = (foos) => from f in foos
                                from b in f.Bars
                                select new BarViewModel { FooName = f.Name, Number = b.Number };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void RangeQuerySucceedsWithEmbeddableStore()
        {
            using (var store = GetDocumentStore())
            {
                VerifyRangeQueryWithStore(store);
            }
        }

        [Fact]
        public void RangeQueryFailsAgainstServer()
        {
            using (var store = GetDocumentStore())
            {
                VerifyRangeQueryWithStore(store);
            }
        }

        private static void VerifyRangeQueryWithStore(IDocumentStore store)
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
                        .ProjectInto<BarViewModel>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    var model = Assert.Single(results);
                    Assert.Equal(2.0f, model.Number);
                }, TimeSpan.FromSeconds(10));
            }
        }
        private static void AssertEventually(Action assertion, TimeSpan timeout)
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
                catch (XunitException ex)
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
