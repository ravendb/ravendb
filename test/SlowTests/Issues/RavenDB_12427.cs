using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12427 : RavenTestBase
    {
        [Fact]
        public void Can_use_Enumerable_Zip_in_indexing_function()
        {
            using (var store = GetDocumentStore())
            {
                new AllTimeRankingIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    
                    session.Store(new WeeklyRanking
                    {
                        Users =  new List<string> { "joe", "doe" },
                        Score = new List<float> { 1.2f, 2.2f}
                    });

                    session.Store(new WeeklyRanking
                    {
                        Users = new List<string> { "joe", "doe" },
                        Score = new List<float> { 1.2f, 2.2f }
                    });


                    session.SaveChanges();

                    var results = session.Query<WeeklyRankingAggregate, AllTimeRankingIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(2.4f, results.First(x => x.Owner == "joe").Score);
                    Assert.Equal(4.4f, results.First(x => x.Owner == "doe").Score);
                }
            }
        }

        private class WeeklyRankingAggregate
        {
            public string Owner { get; set; }
            public float Score { get; set; }
        }


        private class AllTimeRankingIndex : AbstractIndexCreationTask<WeeklyRanking, WeeklyRankingAggregate>
        {
            public AllTimeRankingIndex()
            {
                Map = rankings => from ranking in rankings
                    from x in Enumerable.Zip(ranking.Users, ranking.Score, (u, s) => new WeeklyRankingAggregate { Owner = u, Score = s })
                    select new WeeklyRankingAggregate { Owner = x.Owner, Score = x.Score };

                Reduce = results => from result in results
                    group result by new { result.Owner } into g
                    select new WeeklyRankingAggregate
                    {
                        Owner = g.Key.Owner,
                        Score = g.Sum(x => x.Score)
                    };
            }
        }

        private class WeeklyRanking
        {
            public IEnumerable<string> Users { get; set; }
            public IEnumerable<float> Score { get; set; }
        }
    }
}
