using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenTestSample : RavenTestBase
    {
        private class ClassWithDouble
        {
            public string Id { get; set; }
            public double Rating { get; set; }

            public override string ToString()
            {
                return Rating.ToString();
            }
        }

        private class ClassWithDouble_Index : AbstractIndexCreationTask<ClassWithDouble>
        {
            public ClassWithDouble_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Rating
                              };
            }
        }

        [Fact]
        public void NormalQuery_SortingByDoubleShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new ClassWithDouble_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithDouble
                    {
                        Rating = 5
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 1
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 6
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 4
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 7
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ClassWithDouble, ClassWithDouble_Index>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .OrderBy(x => x.Rating)
                        .ToList();

                    Assert.Equal(results[0].Rating, 1);
                    Assert.Equal(results[1].Rating, 4);
                    Assert.Equal(results[2].Rating, 5);
                    Assert.Equal(results[3].Rating, 6);
                    Assert.Equal(results[4].Rating, 7);
                }
            }
        }

        [Fact]
        public void LuceneQuery_SortingByDoubleDescShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new ClassWithDouble_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithDouble
                    {
                        Rating = 5
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 1
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 6
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 4
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 7
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<ClassWithDouble, ClassWithDouble_Index>()
                        .WaitForNonStaleResultsAsOfNow()
                        .OrderByDescending(x => x.Rating)
                        .ToList();

                    Assert.Equal(results[0].Rating, 7);
                    Assert.Equal(results[1].Rating, 6);
                    Assert.Equal(results[2].Rating, 5);
                    Assert.Equal(results[3].Rating, 4);
                    Assert.Equal(results[4].Rating, 1);
                }
            }
        }

        [Fact]
        public void LuceneQuery_SortingByDoubleShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new ClassWithDouble_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithDouble
                    {
                        Rating = 5
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 1
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 6
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 4
                    });

                    session.Store(new ClassWithDouble
                    {
                        Rating = 7
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<ClassWithDouble, ClassWithDouble_Index>()
                        .WaitForNonStaleResultsAsOfNow()
                        .OrderBy(x => x.Rating)
                        .ToList();

                    Assert.Equal(results[0].Rating, 1);
                    Assert.Equal(results[1].Rating, 4);
                    Assert.Equal(results[2].Rating, 5);
                    Assert.Equal(results[3].Rating, 6);
                    Assert.Equal(results[4].Rating, 7);
                }
            }
        }
    }
}
