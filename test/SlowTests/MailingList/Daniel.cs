using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Daniel : RavenTestBase
    {
        [Fact]
        public void Run()
        {
            using (var store = GetDocumentStore())
            {
                new Matches_PlayerStats().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(Create4x4Match());
                    session.SaveChanges();
                    var stats = session.Query<Matches_PlayerStats.Result, Matches_PlayerStats>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .SingleOrDefault(s => s.Player == "Lars Norbeck");

                    Assert.NotNull(stats);
                }
            }
        }

        private static Match4x4 Create4x4Match()
        {
            var series = new List<Serie4x4>
            {
                new Serie4x4
                {
                    Games = new List<Game4x4>
                    {
                        new Game4x4 {Player = "Tomas Gustavsson", Pins = 160, Score = 0},
                        new Game4x4 {Player = "Markus Norbeck", Pins = 154, Score = 0},
                        new Game4x4 {Player = "Lars Norbeck", Pins = 169, Score = 1},
                        new Game4x4 {Player = "Matz Classon", Pins = 140, Score = 0},
                    }
                },
            };

            var match = new Match4x4
            {
                Location = "Bowl-O-Rama",
                Date = new DateTime(2012, 01, 28),
                Teams =
                    new List<Team4x4>
                    {
                        new Team4x4 {Name = "Fredrikshof C", Score = 6, Series = series},
                        new Team4x4 {Name = "Librex", Score = 14}
                    }
            };
            return match;
        }

        private class Match4x4
        {
            public List<Team4x4> Teams { get; set; }

            public string Id { get; set; }
            public string Location { get; set; }
            public DateTimeOffset Date { get; set; }
        }
        private class Team4x4
        {
            public List<Serie4x4> Series { get; set; }
            public string Name { get; set; }
            public int Score { get; set; }
        }
        private class Serie4x4
        {
            public List<Game4x4> Games { get; set; }
        }
        private class Game4x4
        {
            public int? Strikes { get; set; }
            public int? Misses { get; set; }
            public int? OnePinMisses { get; set; }
            public int? Splits { get; set; }
            public bool CoveredAll { get; set; }
            public string Player { get; set; }
            public int Pins { get; set; }
            public int Score { get; set; }
        }
        private class Matches_PlayerStats : AbstractMultiMapIndexCreationTask<Matches_PlayerStats.Result>
        {
            public Matches_PlayerStats()
            {
                AddMap<Match4x4>(matches => from match in matches
                                            from team in match.Teams
                                            from serie in team.Series
                                            from game in serie.Games
                                            select new
                                            {
                                                game.Player,
                                                game.Pins,
                                                Series = 1,
                                                game.Score,
                                                BestGame = game.Pins,
                                                GamesWithStats = game.Strikes != null ? 1 : 0,
                                                game.Strikes,
                                                game.Misses,
                                                game.OnePinMisses,
                                                game.Splits,
                                                CoveredAll = game.CoveredAll ? 1 : 0
                                            });

                Reduce = results => from result in results
                                    group result by result.Player into stat
                                    select new Result
                                    {
                                        Player = stat.Key,
                                        Pins = stat.Sum(s => s.Pins),
                                        Series = stat.Sum(s => s.Series),
                                        Score = stat.Sum(s => s.Score),
                                        BestGame = stat.Max(s => s.BestGame),
                                        GamesWithStats = stat.Sum(x => x.GamesWithStats),
                                        Strikes = stat.Sum(s => s.Strikes),
                                        Misses = stat.Sum(s => s.Misses),
                                        OnePinMisses = stat.Sum(s => s.OnePinMisses),
                                        Splits = stat.Sum(s => s.Splits),
                                        CoveredAll = stat.Sum(s => s.CoveredAll)
                                    };
            }

            public class Result
            {
                public string Player { get; set; }
                public double Pins { get; set; }
                public double Series { get; set; }
                public double Score { get; set; }
                public int BestGame { get; set; }
                public int GamesWithStats { get; set; }
                public double Strikes { get; set; }
                public double Misses { get; set; }
                public double OnePinMisses { get; set; }
                public double Splits { get; set; }
                public int CoveredAll { get; set; }

                public double AverageScore { get { return Score / Series; } }
                public double AveragePins { get { return Pins / Series; } }
                public double AverageStrikes { get { return Strikes / Math.Max(1, GamesWithStats); } }
                public double AverageMisses { get { return Misses / Math.Max(1, GamesWithStats); } }
                public double AverageOnePinMisses { get { return OnePinMisses / Math.Max(1, GamesWithStats); } }
                public double AverageSplits { get { return Splits / Math.Max(1, GamesWithStats); } }
            }
        }
    }
}
