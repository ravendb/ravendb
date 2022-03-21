using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_6019 : RavenTestBase
    {
        public RavenDB_6019(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(512)]
        public void Can_specify_multiple_map_functions_for_the_same_collection(int numberOfDocs)
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Activity_ByMonth());

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < numberOfDocs; i++)
                    {
                        session.Store(new Question()
                        {
                            CreationDate = new DateTimeOffset(new DateTime(2000, 10, 10)),
                            Answers = new[]
                            {
                                new Answers
                                {
                                    CreationDate = new DateTimeOffset(new DateTime(2000, 10, 10)),
                                    OwnerUserId = 1
                                },
                                new Answers
                                {
                                    CreationDate = new DateTimeOffset(new DateTime(2001, 10, 10)),
                                    OwnerUserId = 2
                                },
                            }
                        });
                    }

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<Activity_ByMonth.Result, Activity_ByMonth>().OrderBy(x => x.Users).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal("2001-10", results[0].Month);
                    Assert.Equal(numberOfDocs, results[0].Users);

                    Assert.Equal("2000-10", results[1].Month);
                    Assert.Equal(numberOfDocs * 2, results[1].Users);
                }
            }
        }

        private class Activity_ByMonth : AbstractMultiMapIndexCreationTask<Activity_ByMonth.Result>
        {
            public class Result
            {
                public int Users;
                public string Month;
            }

            public Activity_ByMonth()
            {
                AddMap<Question>(questions =>
                    from q in questions
                    select new Result
                    {
                        Month = q.CreationDate.ToString("yyyy-MM"),
                        Users = 1
                    });

                AddMap<Question>(questions =>
                   from q in questions
                   from a in q.Answers
                   group a by new // distinct users by month
                   {
                       a.OwnerUserId,
                       Month = a.CreationDate.ToString("yyyy-MM")
                   } into g
                   select new Result
                   {
                       Month = g.Key.Month,
                       Users = g.Count()
                   });

                Reduce = results =>
                    from result in results
                    group result by result.Month into g
                    select new Result
                    {
                        Month = g.Key,
                        Users = g.Sum(x => x.Users),
                    };
            }
        }

        private class Question
        {
            public Answers[] Answers { get; set; }
            public DateTimeOffset CreationDate { get; set; }
        }

        private class Answers
        {
            public DateTimeOffset CreationDate { get; set; }
            public int OwnerUserId { get; set; }
        }
    }
}
