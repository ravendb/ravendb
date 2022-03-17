using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class StreamSortTest : RavenTestBase
    {
        public StreamSortTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Streaming_Results_Should_Sort_Properly()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new FooIndex());

                using (var session = documentStore.OpenSession())
                {
                    var random = new System.Random();

                    for (int i = 0; i < 100; i++)
                        session.Store(new Foo { Num = random.Next(1, 100) });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);


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

        private class Foo
        {
            public string Id { get; set; }
            public int Num { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = foos => from foo in foos
                              select new { foo.Num };

            }
        }
    }
}
