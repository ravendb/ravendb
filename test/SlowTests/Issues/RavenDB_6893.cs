using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Raven.Client.Documents.Indexes;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6893 : RavenTestBase
    {
        public RavenDB_6893(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Map_reduce_on_complex_object()
        {
            using (var store = GetDocumentStore())
            {
                new Index_test1().Execute(store);
                new Index_test2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Items()
                    {
                        gameId = 3165690117,
                        mapId = 11,
                        osQueueId = 420,
                        seasonId = 8,
                        players = new List<players>()
                        {
                            new players()
                            {
                                cId = 254,
                                entries = new List<entries>()
                                {
                                    new entries()
                                    {
                                        c = 498,
                                        role = 4,
                                        synergyC = 1,
                                    }
                                },
                                playerId = 37633534,
                                position = 2,
                                won = true
                            }
                        }
                    });
                    
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Result, Index_test1>().ToList();
                    var results2 = session.Query<Result, Index_test2>().ToList();

                    foreach (var result in new []{results[0], results2[0]})
                    {
                        Assert.Equal(1, result.game_count);
                        Assert.Equal(1, result.game_win_count);
                        Assert.Equal(37633534, result.playerId);

                        Assert.Equal(1, result.seasons.Length);
                        Assert.Equal(8, result.seasons[0].seasonId);
                        Assert.Equal(1, result.seasons[0].season_count);
                        Assert.Equal(1, result.seasons[0].season_win_count);

                        Assert.Equal(420, result.seasons[0].ques[0].osQueueId);
                        Assert.Equal(1, result.seasons[0].ques[0].que_count);
                        Assert.Equal(1, result.seasons[0].ques[0].que_win_count);
                    }
                }
                    
            }
        }

        private class Index_test1 : AbstractIndexCreationTask<Items, Result>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from d in docs.Items from p in d.players select new {    p.playerId,    game_count = 1,    game_win_count = p.won == true ? 1 : 0,    seasons = new []{        new {            d.seasonId,            season_count = 1,            season_win_count = p.won == true ? 1 : 0,            ques = new[]{                new {                    d.osQueueId,                    que_count = 1,                    que_win_count =  p.won == true ? 1 : 0                }            }        }    }}"
                    },
                    Reduce =
                        @"from result in results group result by new {    result.playerId} into g select new {    playerId = g.Key.playerId,    game_count = g.Sum(a => a.game_count),    game_win_count = g.Sum(a => a.game_win_count),    seasons = g.SelectMany(a => a.seasons).GroupBy(a => a.seasonId).Select(a => new {        seasonId = a.Key,        season_count = a.Sum(b => b.season_count),        season_win_count = a.Sum(b => b.season_win_count),        ques = a.SelectMany(b => b.ques).GroupBy(b => b.osQueueId).Select(b => new {            osQueueId = b.Key,            que_count = b.Sum(c => c.que_count),            que_win_count = b.Sum(c => c.que_win_count)        })    }) }"
                };
            }
        }

        private class Index_test2 : AbstractIndexCreationTask<Items, Result>
        {

            public Index_test2()
            {
                Map = docs => from d in docs
                    from p in d.players
                    select new
                    {
                        playerId = p.playerId,
                        game_count = 1,
                        game_win_count = p.won == true ? 1 : 0,
                        seasons = new[]
                        {
                            new Season
                            {
                                seasonId = d.seasonId,
                                season_count = 1,
                                season_win_count = p.won == true ? 1 : 0,
                                ques = new[] {new Que { osQueueId = d.osQueueId, que_count = 1, que_win_count = p.won == true ? 1 : 0}}
                            }
                        }
                    };
                Reduce = results => from result in results
                    group result by new { result.playerId }
                    into g
                    select new
                    {
                        playerId = g.Key.playerId,
                        game_count = g.Sum(a => a.game_count),
                        game_win_count = g.Sum(a => a.game_win_count),
                        seasons = g.SelectMany(a => a.seasons).GroupBy(a => a.seasonId).Select(a => new
                        {
                            seasonId = a.Key,
                            season_count = a.Sum(b => b.season_count),
                            season_win_count = a.Sum(b => b.season_win_count),
                            ques = a.SelectMany(b => b.ques).GroupBy(b => b.osQueueId)
                                .Select(b => new { osQueueId = b.Key, que_count = b.Sum(c => c.que_count), que_win_count = b.Sum(c => c.que_win_count) })
                        })
                    };
            }
        }

        public class Result
        {
            public int playerId { get; set; }
            public int game_count { get; set; }
            public int game_win_count { get; set; }
            public Season[] seasons { get; set; }
        }

        public class Season
        {
            public int seasonId { get; set; }
            public int season_count { get; set; }
            public int season_win_count { get; set; }
            public Que[] ques { get; set; }
        }

        public class Que
        {
            public int osQueueId { get; set; }
            public int que_count { get; set; }
            public int que_win_count { get; set; }
        }

        private class entries
        {
            public int c { get; set; }
            public int role { get; set; }
            public int synergyC { get; set; }
        }

        private class players
        {
            public int cId { get; set; }
            public List<entries> entries { get; set; }
            public int playerId { get; set; }
            public int position { get; set; }
            public bool won { get; set; }
        }

        private class Items
        {
            public long gameId { get; set; }
            public int mapId { get; set; }
            public int osQueueId { get; set; }
            public List<players> players { get; set; }
            public int seasonId { get; set; }
        }
    }
}
