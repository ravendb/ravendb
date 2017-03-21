using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2502 : RavenTestBase
    {
        private class Foo
        {
            public string Name { get; set; }
        }

        private class Bar
        {
            public string Name { get; set; }

            public string ParameterValue { get; set; }
        }

        private class FooTransformer : AbstractTransformerCreationTask<Foo>
        {
            public FooTransformer()
            {
                TransformResults = results => from result in results
                                              select new
                                              {
                                                  result.Name,
                                                  ParameterValue = Parameter("param").Value<string>()
                                              };
            }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  foo.Name
                              };
            }
        }

        [Fact]
        public void TransformerWithParameters_in_streaming_query_should_work()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);
                new FooTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Name = "A"
                    });
                    session.Store(new Foo
                    {
                        Name = "AB"
                    });
                    session.Store(new Foo
                    {
                        Name = "AAB"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Foo, FooIndex>()
                        .TransformWith<FooTransformer, Bar>()
                        .AddTransformerParameter("param", "foobar");

                    using (var stream = session.Advanced.Stream(query))
                    {
                        int count = 0;
                        while (stream.MoveNext())
                        {
                            Assert.Equal(stream.Current.Document.ParameterValue, "foobar");
                            count++;
                        }

                        Assert.Equal(3, count);
                    }
                }
            }
        }
    }
}
