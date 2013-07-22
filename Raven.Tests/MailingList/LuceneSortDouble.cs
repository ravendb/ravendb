using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class RavenTestSample : RavenTestBase
    {
        public class ClassWithDouble
        {
            public string Id { get; set; }
            public double Rating { get; set; }

            public override string ToString()
            {
                return this.Rating.ToString();
            }
        }

        public class ClassWithDouble_Index : AbstractIndexCreationTask<ClassWithDouble>
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
            using (var store = NewDocumentStore())
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
            using (var store = NewDocumentStore())
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
                    var results = session.Advanced.LuceneQuery<ClassWithDouble, ClassWithDouble_Index>()
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
            using (var store = NewDocumentStore())
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
                    var results = session.Advanced.LuceneQuery<ClassWithDouble, ClassWithDouble_Index>()
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