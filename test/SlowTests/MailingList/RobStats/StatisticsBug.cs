// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList.RobStats
{
    public class StatisticsBug : RavenTestBase
    {
        private class Entity
        {
            public string Id { get; set; }
            public string DisplayName { get; set; }
            public string Visibility { get; set; }
            public DateTimeOffset UpdatedAt { get; set; }
        }

        private class Opinion
        {
            public string EntityId { get; set; }
            public bool IsFavorite { get; set; }
        }

        private class Summary
        {
            public string EntityId { get; set; }
            public string DisplayName { get; set; }
            public string Visibility { get; set; }
            public DateTimeOffset UpdatedAt { get; set; }
            public int NumberOfFavorites { get; set; }
        }

        private class TheIndex : AbstractMultiMapIndexCreationTask<Summary>
        {
            public TheIndex()
            {
                AddMap<Opinion>(
                    opinions => from opinion in opinions
                                select new
                                {
                                    opinion.EntityId,
                                    DisplayName = (string)null,
                                    Visibility = (string)null,
                                    UpdatedAt = DateTimeOffset.MinValue,
                                    NumberOfFavorites = opinion.IsFavorite ? 1 : 0,
                                });

                AddMap<Entity>(
                    entities => from entity in entities
                                select new
                                {
                                    EntityId = entity.Id,
                                    entity.DisplayName,
                                    entity.Visibility,
                                    entity.UpdatedAt,
                                    NumberOfFavorites = 0,
                                });

                Reduce = results => from result in results
                                    group result by result.EntityId
                                        into g
                                    select new
                                    {
                                        EntityId = g.Key,
                                        DisplayName = g.Select(x => x.DisplayName).Where(x => x != null).FirstOrDefault(),
                                        Visibility = g.Select(x => x.Visibility).Where(x => x != null).FirstOrDefault(),
                                        UpdatedAt = g.Max(x => (DateTimeOffset)x.UpdatedAt),
                                        NumberOfFavorites = g.Sum(x => x.NumberOfFavorites),
                                    };
            }
        }

        [Fact]
        public void Should_get_stats_whe_using_lazy()
        {
            using (var store = GetDocumentStore())
            {
                new TheIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 15; i++)
                    {
                        var entity = new Entity
                        {
                            DisplayName = "Entity " + i,
                            UpdatedAt = DateTimeOffset.Now,
                            Visibility = "Visible"
                        };

                        session.Store(entity);

                        var opinion = new Opinion
                        {
                            EntityId = entity.Id,
                            IsFavorite = i % 2 == 0
                        };

                        session.Store(opinion);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<Summary, TheIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Visibility == "Visible")
                        .OrderByDescending(x => x.UpdatedAt);

                    var pagedQuery = query
                        .Skip(0)
                        .Take(10)
                        .Lazily();


                    var items = pagedQuery.Value.ToArray();
                    Assert.Equal(15, stats.TotalResults);
                    Assert.Equal(10, items.Length);
                }
            }
        }
    }
}
