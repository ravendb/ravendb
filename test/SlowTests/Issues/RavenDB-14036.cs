using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14036 : RavenTestBase
    {
        public RavenDB_14036(ITestOutputHelper output) : base(output)
        {
        }

        private class MyIndex : AbstractMultiMapIndexCreationTask<MyIndex.Result>
        {
            public class Result
            {
                public string UdcId { get; set; }

                public long Long { get; set; }

                public double Double { get; set; }

            }

            public MyIndex()
            {
                AddMap<MyDocument>(docs => from udc in docs
                    select new Result
                    {
                        UdcId = udc.Id,
                        Long = udc.LongNumber,
                        Double = udc.DoubleNumber
                    });

                Store(r => r.UdcId, FieldStorage.Yes);
                Store(r => r.Long, FieldStorage.Yes);
                Store(r => r.Double, FieldStorage.Yes);
            }
        }

        private class MyDocument
        {
            public string Id { get; set; }

            public string Text { get; set; }

            public long LongNumber { get; set;  }

            public double DoubleNumber { get; set; }

        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProjectStoredStringFieldThatLooksLikeLargeNumberUsingJsProjection(Options options)
        {
            var id = "080034951900720231";

            using (var store = GetDocumentStore(options))
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyDocument
                    {
                        Id = id,
                        Text = "2019/72023"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from a in session.Query<MyIndex.Result, MyIndex>()
                                let udc = RavenQuery.Load<MyDocument>(a.UdcId)
                                select new
                                {
                                    udc.Id, 
                                    a.UdcId
                                };

                    var test = query.ToArray();

                    Assert.Equal(id, test[0].Id);
                    Assert.Equal(id, test[0].UdcId);
                }
            }
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanExtractStoredNumberFieldsUsingJsProjection(Options options)
        {
            long l = 9007199254740990;
            double d = 1.7976931348623157e+308;

            using (var store = GetDocumentStore(options))
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyDocument
                    {
                        LongNumber = l,
                        DoubleNumber = d
                    });

                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from a in session.Query<MyIndex.Result, MyIndex>()
                        let udc = "foo" // creates JS projection 
                        select new
                        {
                            a.Long,
                            a.Double
                        };

                    var test = query.ToArray();

                    Assert.Equal(l, test[0].Long);
                    Assert.Equal(d, test[0].Double);

                }
            }

        }
    }
}
