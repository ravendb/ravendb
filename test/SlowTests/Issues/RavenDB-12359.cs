using Tests.Infrastructure;
using System;
using FastTests;
using Raven.Client.Documents;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12359 : RavenTestBase
    {
        public RavenDB_12359(ITestOutputHelper output) : base(output)
        {
        }

        private class MyDoc
        {
            public string Id { get; set; }
            public int? NullableInt { get; set; }
        }

        private class WithHasValue
        {
            public bool? HasValue;
#pragma warning disable 649
            public object SomeProp;
#pragma warning restore 649
        }

        private void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                s.Store(new MyDoc
                {
                    Id = "1",
                    NullableInt = null
                });
                s.Store(new MyDoc
                {
                    Id = "2",
                    NullableInt = 1
                });
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var docs = s.Query<MyDoc>().OrderBy(i => i.Id).ToList();
                Assert.Equal(docs.Count, 2);
                Assert.Null(docs[0].NullableInt);
                Assert.Equal(1, docs[1].NullableInt);
            }

        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProjectHasValuePropertyOfNullable(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                                orderby d.Id
                                select new
                                {
                                    HasValue = d.NullableInt.HasValue
                                };

                    Assert.Equal("from 'MyDocs' as d order by id() select { HasValue : d?.NullableInt != null }"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue);
                    Assert.True(results[1].HasValue);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProjectHasValuePropertyOfNullable2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                                orderby d.Id
                                select new WithHasValue
                                {
                                    HasValue = d.NullableInt.HasValue
                                };

                    Assert.Equal("from 'MyDocs' as d order by id() select { HasValue : d?.NullableInt != null }"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue);
                    Assert.True(results[1].HasValue);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void TestGreaterThanOrEqualToZero(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var query = from d in s.Query<MyDoc>()
                                select new
                                {
                                    HasValue = d.NullableInt >= 0
                                };

                    Assert.Equal("from 'MyDocs' as d select { " +
                                 "HasValue : d?.NullableInt>0||d?.NullableInt===0 }"
                                 , query.ToString());

                    var results = query.ToList();

                    Assert.Equal(results.Count, 2);
                    Assert.False(results[0].HasValue);
                    Assert.True(results[1].HasValue);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void NullableDateTimeProjectionUTC(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var nowUTC = DateTime.UtcNow;
                    newSession.Store(new Person
                    {
                        BirthDate = nowUTC
                    });
                    newSession.SaveChanges();

                    var query = newSession.Query<Person>()
                        .Select(x => new Projection
                        {
                            BirthHour = x.BirthDate != null ? x.BirthDate.Value.Hour : (int?)null,
                            BirthDay = x.BirthDate != null ? x.BirthDate.Value.Day : (int?)null,
                            BirthMonth = x.BirthDate != null ? x.BirthDate.Value.Month : (int?)null,
                            BirthYear = x.BirthDate != null ? x.BirthDate.Value.Year : (int?)null
                        });

                    var queryString = query.ToString();
                    Assert.Equal("from 'People' as x select { " +  
                                 "BirthHour : x?.BirthDate!=null?(x?.BirthDate?.getHours()):null, " +
                                 "BirthDay : x?.BirthDate!=null?(x?.BirthDate?.getDate()):null, " +
                                 "BirthMonth : x?.BirthDate!=null?(x?.BirthDate?.getMonth()+1):null, " +
                                 "BirthYear : x?.BirthDate!=null?(x?.BirthDate?.getFullYear()):null }", queryString);

                    var list = query.ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal(nowUTC.Hour, list[0].BirthHour);
                    Assert.Equal(nowUTC.Day, list[0].BirthDay);
                    Assert.Equal(nowUTC.Month, list[0].BirthMonth);
                    Assert.Equal(nowUTC.Year, list[0].BirthYear);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void NullableDateTimeProjectionLocal(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var now = DateTime.Now;
                    newSession.Store(new Person
                    {
                        BirthDate = now
                    });
                    newSession.SaveChanges();

                    var query = newSession.Query<Person>()
                        .Select(x => new Projection
                        {
                            BirthHour = x.BirthDate != null ? x.BirthDate.Value.Hour : (int?)null,
                            BirthDay = x.BirthDate != null ? x.BirthDate.Value.Day : (int?)null,
                            BirthMonth = x.BirthDate != null ? x.BirthDate.Value.Month : (int?)null,
                            BirthYear = x.BirthDate != null ? x.BirthDate.Value.Year : (int?)null
                        });

                    var queryString = query.ToString();
                    Assert.Equal("from 'People' as x select { " +
                                 "BirthHour : x?.BirthDate!=null?(x?.BirthDate?.getHours()):null, " +
                                 "BirthDay : x?.BirthDate!=null?(x?.BirthDate?.getDate()):null, " +
                                 "BirthMonth : x?.BirthDate!=null?(x?.BirthDate?.getMonth()+1):null, " +
                                 "BirthYear : x?.BirthDate!=null?(x?.BirthDate?.getFullYear()):null }", queryString);

                    var list = query.ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal(now.Hour, list[0].BirthHour);
                    Assert.Equal(now.Day, list[0].BirthDay);
                    Assert.Equal(now.Month, list[0].BirthMonth);
                    Assert.Equal(now.Year, list[0].BirthYear);
                }
            }
        }

        [Fact]
        public void NullableDateTimeValueProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var nowUTC = DateTime.UtcNow;
                    var sinceBirth = nowUTC - new DateTime(1985, 8, 13);
                    newSession.Store(new Person
                    {
                        BirthDate = nowUTC,
                        SinceBirth = sinceBirth
                    });
                    newSession.SaveChanges();

                    var query = newSession.Query<Person>()
                        .Select(x => new Projection
                        {
                            BirthDay = x.BirthDate.Value.Day,
                            BirthMonth = x.BirthDate.Value.Month,
                            BirthYear = x.BirthDate.Value.Year,
                            SinceBirthTotalMilliseconds = x.SinceBirth.Value.TotalMilliseconds
                        });

                    var queryString = query.ToString();
                    Assert.Equal("from 'People' select " +
                                 "BirthDate.Day as BirthDay, " +
                                 "BirthDate.Month as BirthMonth, " +
                                 "BirthDate.Year as BirthYear, " +
                                 "SinceBirth.TotalMilliseconds as SinceBirthTotalMilliseconds", queryString);

                    var list = query.ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal(nowUTC.Day, list[0].BirthDay);
                    Assert.Equal(nowUTC.Month, list[0].BirthMonth);
                    Assert.Equal(nowUTC.Year, list[0].BirthYear);
                    Assert.Equal(sinceBirth.TotalMilliseconds, list[0].SinceBirthTotalMilliseconds);
                }
            }
        }

        private class Person
        {
            public DateTime? BirthDate { get; set; }

            public TimeSpan? SinceBirth { get; set; }
        }

        private class Projection
        {
            public int? BirthHour { get; set; }

            public int? BirthDay { get; set; }

            public int? BirthMonth { get; set; }

            public int? BirthYear { get; set; }

            public double SinceBirthTotalMilliseconds { get; set; }
        }
    }
}
